/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.hdfs.protocol.datatransfer.InvalidEncryptionKeyException;
import org.apache.hadoop.hdfs.DFSUtilClient.CorruptedBlocks;
import org.apache.hadoop.hdfs.StripeReader.BlockReaderInfo;
import org.apache.hadoop.hdfs.StripeReader.ReaderRetryPolicy;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.hdfs.util.StripedBlockUtil.AlignedStripe;
import org.apache.hadoop.hdfs.util.StripedBlockUtil.StripeRange;
import org.apache.hadoop.io.ByteBufferPool;

import org.apache.hadoop.io.ElasticByteBufferPool;
import org.apache.hadoop.io.erasurecode.CodecUtil;

import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;

import static org.apache.hadoop.hdfs.util.IOUtilsClient.updateReadStatistics;

/**
 * DFSStripedInputStream reads from striped block groups.
 */
@InterfaceAudience.Private
public class DFSStripedInputStream extends DFSInputStream {

  protected static final ByteBufferPool BUFFER_POOL = new ElasticByteBufferPool();
  protected final BlockReaderInfo[] blockReaders;
  protected final int cellSize;
  protected final short dataBlkNum;
  protected short parityBlkNum;
  protected final int totalBlkNum;
  /** will be null if ecTransitioned = false */
  protected List<LocatedRedundancyGroup> redundancyGroup;
  /** the buffer for a complete stripe. */
  protected ByteBuffer curStripeBuf;
  /** the buffer for a redundancy group stripe. */
  private ByteBuffer redundancyStripeBuf;
  @VisibleForTesting
  protected ByteBuffer parityBuf;
  protected final ErasureCodingPolicy ecPolicy;
  protected final int stripeWidth;
  protected RawErasureDecoder decoder;
  protected long requestStartTime = 0L;
  protected boolean degradedRead = false;
  protected int missingUnits = 0;

  /**
   * Indicate the start/end offset of the current buffered stripe in the
   * block group.
   */
  protected StripeRange curStripeRange;

  /**
   * When warning the user of a lost block in striping mode, we remember the
   * dead nodes we've logged. All other striping blocks on these nodes can be
   * considered lost too, and we don't want to log a warning for each of them.
   * This is to prevent the log from being too verbose. Refer to HDFS-8920.
   *
   * To minimize the overhead, we only store the datanodeUuid in this set
   */
  protected final Set<String> warnedNodes =
      Collections.newSetFromMap(new ConcurrentHashMap<>());

  DFSStripedInputStream(DFSClient dfsClient, String src,
      boolean verifyChecksum, ErasureCodingPolicy ecPolicy,
      LocatedBlocks locatedBlocks) throws IOException {
    super(dfsClient, src, verifyChecksum, locatedBlocks);

    this.readStatistics.setBlockType(BlockType.STRIPED);
    assert ecPolicy != null;
    this.cellSize = ecPolicy.getCellSize();

    // for transitioned files, use the original EC scheme as that's the data format
    this.ecPolicy = ecPolicy;
    dataBlkNum = (short) ecPolicy.getNumDataUnits();
    parityBlkNum = (short) ecPolicy.getNumParityUnits();
    stripeWidth = ecPolicy.getStripeWidth();

    // assume only one stripe failure at a time so only one set of parities
    // need at least as many spots for the stripe for reading in normal
    // and then at least as many spots for the group if degraded mode reads
    totalBlkNum = Math.max(dataBlkNum, stripeWidth) + parityBlkNum;
    blockReaders = new BlockReaderInfo[totalBlkNum];
    curStripeRange = new StripeRange(0, 0);

    ErasureCoderOptions coderOptions = new ErasureCoderOptions(
        dataBlkNum, parityBlkNum);
    decoder = CodecUtil.createRawDecoder(dfsClient.getConfiguration(),
        ecPolicy.getCodecName(), coderOptions);

    if (DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug("Creating an striped input stream for file " + src);
    }
  }

  private boolean useDirectBuffer() {
    return decoder.preferDirectBuffer();
  }

  private void resetCurStripeBuffer(boolean shouldAllocateBuf) {
    if (shouldAllocateBuf && curStripeBuf == null) {
      curStripeBuf = BUFFER_POOL.getBuffer(useDirectBuffer(),
          cellSize * dataBlkNum);
    }
    if (curStripeBuf != null) {
      curStripeBuf.clear();
    }
    curStripeRange = new StripeRange(0, 0);
  }

  protected synchronized ByteBuffer getParityBuffer() {
    if (parityBuf == null) {
      parityBuf = BUFFER_POOL.getBuffer(useDirectBuffer(),
          cellSize * parityBlkNum);
    }
    parityBuf.clear();
    return parityBuf;
  }

  protected ByteBuffer getCurStripeBuf() {
    return curStripeBuf;
  }

  protected String getSrc() {
    return src;
  }

  protected LocatedBlocks getLocatedBlocks() {
    return locatedBlocks;
  }

  protected ByteBufferPool getBufferPool() {
    return BUFFER_POOL;
  }

  protected ThreadPoolExecutor getStripedReadsThreadPool(){
    return dfsClient.getStripedReadsThreadPool();
  }
  /**
   * When seeking into a new block group, create blockReader for each internal
   * block in the group.
   */
  @VisibleForTesting
  synchronized void blockSeekTo(long target) throws IOException {
    if (target >= getFileLength()) {
      throw new IOException("Attempted to read past end of file");
    }

    // Will be getting a new BlockReader.
    closeCurrentBlockReaders();

    // Compute desired striped block group
    LocatedStripedBlock targetBlockGroup = getBlockGroupAt(target);
    // Update current position
    this.pos = target;
    this.blockEnd = targetBlockGroup.getStartOffset() +
        targetBlockGroup.getBlockSize() - 1;
    currentLocatedBlock = targetBlockGroup;
  }

  @Override
  public synchronized void close() throws IOException {
    try {
      super.close();
    } finally {
      if (curStripeBuf != null) {
        BUFFER_POOL.putBuffer(curStripeBuf);
        curStripeBuf = null;
      }
      if (parityBuf != null) {
        BUFFER_POOL.putBuffer(parityBuf);
        parityBuf = null;
      }
      if (decoder != null) {
        decoder.release();
        decoder = null;
      }
    }
  }

  /**
   * Extend the super method with the logic of switching between cells.
   * When reaching the end of a cell, proceed to the next cell and read it
   * with the next blockReader.
   */
  @Override
  protected void closeCurrentBlockReaders() {
    resetCurStripeBuffer(false);
    if (blockReaders ==  null || blockReaders.length == 0) {
      return;
    }
    for (int i = 0; i < totalBlkNum; i++) {
      closeReader(blockReaders[i]);
      blockReaders[i] = null;
    }
    blockEnd = -1;
  }

  protected void closeReader(BlockReaderInfo readerInfo) {
    if (readerInfo != null) {
      if (readerInfo.reader != null) {
        try {
          readerInfo.reader.close();
        } catch (Throwable ignored) {
        }
      }
      readerInfo.skip();
    }
  }

  private long getOffsetInBlockGroup() {
    return getOffsetInBlockGroup(pos);
  }

  private long getOffsetInBlockGroup(long pos) {
    return pos - currentLocatedBlock.getStartOffset();
  }

  boolean createBlockReader(LocatedBlock block, long offsetInBlock, long spanInBlock,
      LocatedBlock[] targetBlocks, BlockReaderInfo[] readerInfos,
      int chunkIndex) throws IOException {
    BlockReader reader = null;
    final ReaderRetryPolicy retry = new ReaderRetryPolicy();
    DFSInputStream.DNAddrPair dnInfo =
        new DFSInputStream.DNAddrPair(null, null, null, null);

    while (true) {
      final long start = System.currentTimeMillis();
      try {
        // the cached block location might have been re-fetched, so always
        // get it from cache.
        // Tim: don't refetch from cache here, this resets the location
        // block = refreshLocatedBlock(block);
        // targetBlocks[chunkIndex] = block;

        // internal block has one location, just rule out the deadNodes
        dnInfo = getBestNodeDNAddrPair(block, null);
        if (dnInfo == null) {
          break;
        }
        if (spanInBlock == 0) {
          spanInBlock = block.getBlockSize();
        }
        reader = getBlockReader(block, offsetInBlock,
            spanInBlock - offsetInBlock,
            dnInfo.addr, dnInfo.storageType, dnInfo.info);
        DFSClient.LOG.debug("Created block reader in {} ms", System.currentTimeMillis() - start);
      } catch (IOException e) {
        if (e instanceof InvalidEncryptionKeyException &&
            retry.shouldRefetchEncryptionKey()) {
          DFSClient.LOG.info("Will fetch a new encryption key and retry, "
              + "encryption key was invalid when connecting to " + dnInfo.addr
              + " : " + e);
          dfsClient.clearDataEncryptionKey();
          retry.refetchEncryptionKey();
        } else if (retry.shouldRefetchToken() &&
            tokenRefetchNeeded(e, dnInfo.addr)) {
          fetchBlockAt(block.getStartOffset());
          retry.refetchToken();
        } else {
          //TODO: handles connection issues
          DFSClient.LOG.info("Attempting to connect took {} ms", System.currentTimeMillis() - start);
          DFSClient.LOG.warn("Failed to connect to " + dnInfo.addr + " for " +
              "block" + block.getBlock());
          // re-fetch the block in case the block has been moved
          fetchBlockAt(block.getStartOffset());
          addToLocalDeadNodes(dnInfo.info);
        }
      }
      if (reader != null) {
        readerInfos[chunkIndex] =
            new BlockReaderInfo(reader, dnInfo.info, offsetInBlock);
        return true;
      }
    }
    return false;
  }

  /**
   * Read a new stripe covering the current position, and store the data in the
   * {@link #curStripeBuf}.
   */
  private void readOneStripe(CorruptedBlocks corruptedBlocks)
      throws IOException {
    resetCurStripeBuffer(true);

    // compute stripe range based on pos
    final long offsetInBlockGroup = getOffsetInBlockGroup();
    final long stripeLen = cellSize * dataBlkNum;
    final int stripeIndex = (int) (offsetInBlockGroup / stripeLen);
    final int stripeBufOffset = (int) (offsetInBlockGroup % stripeLen);
    final int stripeLimit = (int) Math.min(currentLocatedBlock.getBlockSize()
        - (stripeIndex * stripeLen), stripeLen);
    StripeRange stripeRange =
        new StripeRange(offsetInBlockGroup, stripeLimit - stripeBufOffset);

    LocatedStripedBlock blockGroup = (LocatedStripedBlock) currentLocatedBlock;
    AlignedStripe[] stripes = StripedBlockUtil.divideOneStripe(ecPolicy,
        cellSize, blockGroup, offsetInBlockGroup,
        offsetInBlockGroup + stripeRange.getLength() - 1, curStripeBuf);
    final LocatedBlock[] blks = StripedBlockUtil.parseStripedBlockGroup(
        blockGroup, cellSize, dataBlkNum, parityBlkNum);
    final LocatedBlock[] redundancyBlks = null; // TODO: parse out the subrange of relevant blocks
    // read the whole stripe
    for (AlignedStripe stripe : stripes) {
      // Parse group to get chosen DN location
      StripeReader sreader = new StatefulStripeReader(stripe, ecPolicy, blks,
          blockReaders, corruptedBlocks, decoder, this);
      sreader.readStripe();
    }
    curStripeBuf.position(stripeBufOffset);
    curStripeBuf.limit(stripeLimit);
    curStripeRange = stripeRange;
  }

  /**
   * Update read statistics. Note that this has to be done on the thread that
   * initiates the read, rather than inside each async thread, for
   * {@link org.apache.hadoop.fs.FileSystem.Statistics} to work correctly with
   * its ThreadLocal.
   *
   * @param stats striped read stats
   */
  void updateReadStats(final StripedBlockUtil.BlockReadStats stats) {
    if (stats == null) {
      return;
    }
    updateReadStatistics(readStatistics, stats.getBytesRead(),
        stats.isShortCircuit(), stats.getNetworkDistance());
    dfsClient.updateFileSystemReadStats(stats.getNetworkDistance(),
        stats.getBytesRead());
    assert readStatistics.getBlockType() == BlockType.STRIPED;
    dfsClient.updateFileSystemECReadStats(stats.getBytesRead());
  }

  /**
   * Seek to a new arbitrary location.
   */
  @Override
  public synchronized void seek(long targetPos) throws IOException {
    if (targetPos > getFileLength()) {
      throw new EOFException("Cannot seek after EOF");
    }
    if (targetPos < 0) {
      throw new EOFException("Cannot seek to negative offset");
    }
    if (closed.get()) {
      throw new IOException("Stream is closed!");
    }
    if (targetPos <= blockEnd) {
      final long targetOffsetInBlk = getOffsetInBlockGroup(targetPos);
      if (curStripeRange.include(targetOffsetInBlk)) {
        int bufOffset = getStripedBufOffset(targetOffsetInBlk);
        curStripeBuf.position(bufOffset);
        pos = targetPos;
        return;
      }
    }
    pos = targetPos;
    blockEnd = -1;
  }

  private int getStripedBufOffset(long offsetInBlockGroup) {
    final long stripeLen = cellSize * dataBlkNum;
    // compute the position in the curStripeBuf based on "pos"
    return (int) (offsetInBlockGroup % stripeLen);
  }

  @Override
  public synchronized boolean seekToNewSource(long targetPos)
      throws IOException {
    return false;
  }

  @Override
  protected synchronized int readWithStrategy(ReaderStrategy strategy)
      throws IOException {
    dfsClient.checkOpen();
    if (closed.get()) {
      throw new IOException("Stream closed");
    }

    int len = strategy.getTargetLength();
    CorruptedBlocks corruptedBlocks = new CorruptedBlocks();
    if (pos < getFileLength()) {
      try {
        if (pos > blockEnd) {
          blockSeekTo(pos);
        }
        int realLen = (int) Math.min(len, (blockEnd - pos + 1L));
        synchronized (infoLock) {
          if (locatedBlocks.isLastBlockComplete()) {
            realLen = (int) Math.min(realLen,
                locatedBlocks.getFileLength() - pos);
          }
        }

        /** Number of bytes already read into buffer */
        int result = 0;
        while (result < realLen) {
          if (!curStripeRange.include(getOffsetInBlockGroup())) {
            long startTime = System.currentTimeMillis();
            readOneStripe(corruptedBlocks);
          }
          int ret = copyToTargetBuf(strategy, realLen - result);
          result += ret;
          pos += ret;
        }
        return result;
      } finally {
        // Check if need to report block replicas corruption either read
        // was successful or ChecksumException occurred.
        reportCheckSumFailure(corruptedBlocks, getCurrentBlockLocationsLength(),
            true);
      }
    }
    return -1;
  }

  /**
   * Copy the data from {@link #curStripeBuf} into the given buffer.
   * @param strategy the ReaderStrategy containing the given buffer
   * @param length target length
   * @return number of bytes copied
   */
  private int copyToTargetBuf(ReaderStrategy strategy, int length) {
    final long offsetInBlk = getOffsetInBlockGroup();
    int bufOffset = getStripedBufOffset(offsetInBlk);
    curStripeBuf.position(bufOffset);
    return strategy.readFromBuffer(curStripeBuf,
        Math.min(length, curStripeBuf.remaining()));
  }

  /**
   * The super method {@link DFSInputStream#refreshLocatedBlock} refreshes
   * cached LocatedBlock by executing {@link DFSInputStream#getBlockAt} again.
   * This method extends the logic by first remembering the index of the
   * internal block, and re-parsing the refreshed block group with the same
   * index.
   */
  @Override
  protected LocatedBlock refreshLocatedBlock(LocatedBlock block)
      throws IOException {
//    DFSClient.LOG.info("Calling refreshlocatedblock in stripedinputstream");
    int idx = StripedBlockUtil.getBlockIndex(block.getBlock().getLocalBlock());
    LocatedBlock lb = getBlockGroupAt(block.getStartOffset());
    // If indexing information is returned, iterate through the index array
    // to find the entry for position idx in the group
    LocatedStripedBlock lsb = (LocatedStripedBlock) lb;
    int i = 0;
    for (; i < lsb.getBlockIndices().length; i++) {
      if (lsb.getBlockIndices()[i] == idx) {
        break;
      }
    }
    if (DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug("refreshLocatedBlock for striped blocks, offset="
          + block.getStartOffset() + ". Obtained block " + lb + ", idx=" + idx);
    }
    return StripedBlockUtil.constructInternalBlock(
        lsb, i, cellSize, dataBlkNum, idx);
  }

  private LocatedStripedBlock getBlockGroupAt(long offset) throws IOException {
    LocatedBlock lb = super.getBlockAt(offset);
    assert lb instanceof LocatedStripedBlock : "NameNode" +
        " should return a LocatedStripedBlock for a striped file";
    return (LocatedStripedBlock)lb;
  }

  private LocatedStripedBlock getParityGroupAt(int index) throws IOException {
    LocatedBlock lb = super.locatedBlocks.getParityBlocks().get(index);
    assert lb instanceof LocatedStripedBlock : "NameNode" +
        " should return a LocatedStripedBlock for a striped file";
    return (LocatedStripedBlock)lb;
  }

  /**
   * Real implementation of pread.
   */
  @Override
  protected void fetchBlockByteRange(LocatedBlock block, long start,
      long end, ByteBuffer buf, CorruptedBlocks corruptedBlocks)
      throws IOException {
    final long startTime = System.currentTimeMillis();
    if (requestStartTime == 0L) {
      // track total request start time
      requestStartTime = startTime;
    }
    LocatedStripedBlock dataStripe = getBlockGroupAt(block.getStartOffset());

    // determine metadata ranges
    int actualDataWidth = Math.max(dataBlkNum, stripeWidth);
    int indexOfStripe = locatedBlocks.findBlock(block.getStartOffset());
    int stripeWidth = ecPolicy.getStripeWidth();
    int groupWidth = ecPolicy.getNumDataUnits();
    int numStripesPerGroup = Math.max(1, groupWidth / stripeWidth);
    int indexOfGroup = indexOfStripe / numStripesPerGroup;
    int indexOfFirstStripeOfGroup = indexOfGroup * numStripesPerGroup;
    int indexOfStripeInGroup = indexOfStripe % numStripesPerGroup;
    int stripeStartIndex = indexOfStripeInGroup * stripeWidth; // index within the blk readers where the actual stripe begins

    // it's possible this is less than 1 in which case that's fine, it's encompassed in stripe
    LocatedStripedBlock[] dataStripes = new LocatedStripedBlock[numStripesPerGroup];
    for (short i = 0; i < numStripesPerGroup; i++) {
      // get the other data stripes that are a part of this group
      dataStripes[i] = (LocatedStripedBlock) locatedBlocks.get(indexOfFirstStripeOfGroup + i);
    }

    // now grab the parity group associated with the stripes
    // the index is the current index divided by the total size of stripes?
    // the group index = group width / stripe width
    LocatedStripedBlock parityGroup = getParityGroupAt(indexOfGroup);

    // shift down chunks to align stripe to correct blocks
    AlignedStripe[] stripes = StripedBlockUtil.divideByteRangeIntoStripes(
      ecPolicy, cellSize, dataStripe, start, end, buf);
    if (numStripesPerGroup > 1 && indexOfStripeInGroup > 0) {
      for (AlignedStripe stripe : stripes) {
        for (int i = 0; i < stripeWidth; i++) {
          stripe.chunks[stripeStartIndex + i] = stripe.chunks[i];
          stripe.chunks[i] = null;
        }
      }
    }

    final LocatedBlock[] blks = StripedBlockUtil.parseStripedBlockGroupsWithParities(
        dataStripes, parityGroup, cellSize, stripeWidth, parityBlkNum);
    final BlockReaderInfo[] preaderInfos = new BlockReaderInfo[totalBlkNum];
    long readTo = -1;
    for (AlignedStripe stripe : stripes) {
      readTo = Math.max(readTo, stripe.getOffsetInBlock() + stripe.getSpanInBlock());
    }
    try {
      for (AlignedStripe stripe : stripes) {
        // Parse group to get chosen DN location
        StripeReader preader = new PositionStripeReader(stripe, ecPolicy, blks,
            preaderInfos, corruptedBlocks, decoder, this);
        preader.readTo = readTo;
        preader.stripeStartIndex = stripeStartIndex;
        preader.stripeWidth = stripeWidth;
        preader.actualDataWidth = actualDataWidth;
        try {
          preader.readStripe();
        } finally {
          degradedRead |= preader.degradedRead;
          missingUnits += preader.missingUnits;
          preader.close();
        }
      }
      buf.position(buf.position() + (int)(end - start + 1));
    } finally {
      for (BlockReaderInfo preaderInfo : preaderInfos) {
        closeReader(preaderInfo);
      }
      if (this.isFinalRequest) {
        // last request from client, print out total request servicing time
        DFSClient.LOG.info("Completed real EC striped read | scheme = {}-wide stripe : {}-wide group " +
                "| degraded mode = {}, missing = {} units, striped read in {} ms",
            stripeWidth, dataBlkNum, degradedRead, missingUnits, System.currentTimeMillis() - requestStartTime);
        // reset request time
        requestStartTime = 0L;
      }
    }
  }

  @Override
  protected void reportLostBlock(LocatedBlock lostBlock,
      Collection<DatanodeInfo> ignoredNodes) {
    DatanodeInfo[] nodes = lostBlock.getLocations();
    if (nodes != null && nodes.length > 0) {
      List<String> dnUUIDs = new ArrayList<>();
      for (DatanodeInfo node : nodes) {
        dnUUIDs.add(node.getDatanodeUuid());
      }
      if (!warnedNodes.containsAll(dnUUIDs)) {
        DFSClient.LOG.warn(Arrays.toString(nodes) + " are unavailable and " +
            "all striping blocks on them are lost. " +
            "IgnoredNodes = " + ignoredNodes);
        warnedNodes.addAll(dnUUIDs);
      }
    } else {
      super.reportLostBlock(lostBlock, ignoredNodes);
    }
  }

  /**
   * May need online read recovery, zero-copy read doesn't make
   * sense, so don't support it.
   */
  @Override
  public synchronized ByteBuffer read(ByteBufferPool bufferPool,
      int maxLength, EnumSet<ReadOption> opts)
          throws IOException, UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "Not support enhanced byte buffer access.");
  }

  @Override
  public synchronized void releaseBuffer(ByteBuffer buffer) {
    throw new UnsupportedOperationException(
        "Not support enhanced byte buffer access.");
  }

  @Override
  public synchronized void unbuffer() {
    super.unbuffer();
    if (curStripeBuf != null) {
      BUFFER_POOL.putBuffer(curStripeBuf);
      curStripeBuf = null;
    }
    if (parityBuf != null) {
      BUFFER_POOL.putBuffer(parityBuf);
      parityBuf = null;
    }
  }
}
