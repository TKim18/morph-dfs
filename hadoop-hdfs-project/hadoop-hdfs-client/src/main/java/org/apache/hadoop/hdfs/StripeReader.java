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

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.hdfs.util.StripedBlockUtil.BlockReadStats;
import org.apache.hadoop.hdfs.util.StripedBlockUtil.StripingChunk;
import org.apache.hadoop.hdfs.util.StripedBlockUtil.AlignedStripe;
import org.apache.hadoop.hdfs.util.StripedBlockUtil.StripingChunkReadResult;
import org.apache.hadoop.io.erasurecode.ECChunk;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;
import org.apache.hadoop.hdfs.DFSUtilClient.CorruptedBlocks;
import org.apache.hadoop.util.Time;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;

/**
 * The reader for reading a complete {@link StripedBlockUtil.AlignedStripe}.
 * Note that an {@link StripedBlockUtil.AlignedStripe} may cross multiple
 * stripes with cellSize width.
 */
abstract class StripeReader {

  static class ReaderRetryPolicy {
    private int fetchEncryptionKeyTimes = 1;
    private int fetchTokenTimes = 1;

    void refetchEncryptionKey() {
      fetchEncryptionKeyTimes--;
    }

    void refetchToken() {
      fetchTokenTimes--;
    }

    boolean shouldRefetchEncryptionKey() {
      return fetchEncryptionKeyTimes > 0;
    }

    boolean shouldRefetchToken() {
      return fetchTokenTimes > 0;
    }
  }

  static class BlockReaderInfo {
    final BlockReader reader;
    final DatanodeInfo datanode;
    /**
     * when initializing block readers, their starting offsets are set to the
     * same number: the smallest internal block offsets among all the readers.
     * This is because it is possible that for some internal blocks we have to
     * read "backwards" for decoding purpose. We thus use this offset array to
     * track offsets for all the block readers so that we can skip data if
     * necessary.
     */
    long blockReaderOffset;
    /**
     * We use this field to indicate whether we should use this reader. In case
     * we hit any issue with this reader, we set this field to true and avoid
     * using it for the next stripe.
     */
    boolean shouldSkip = false;

    BlockReaderInfo(BlockReader reader, DatanodeInfo dn, long offset) {
      this.reader = reader;
      this.datanode = dn;
      this.blockReaderOffset = offset;
    }

    void setOffset(long offset) {
      this.blockReaderOffset = offset;
    }

    void skip() {
      this.shouldSkip = true;
    }
  }

  private final Map<Future<BlockReadStats>, Integer> futures =
      new HashMap<>();
  protected final AlignedStripe alignedStripe;
  private final CompletionService<BlockReadStats> service;
  protected final LocatedBlock[] targetBlocks;
  protected final CorruptedBlocks corruptedBlocks;
  protected final BlockReaderInfo[] readerInfos;
  protected final ErasureCodingPolicy ecPolicy;
  protected final short dataBlkNum;
  protected final short parityBlkNum;
  protected final int cellSize;
  protected final RawErasureDecoder decoder;
  protected final DFSStripedInputStream dfsStripedInputStream;
  boolean degradedRead = false;
  int missingUnits = 0;
  List<DatanodeInfo> failedNodes = new ArrayList<>();
  long readTo = 0;
  int stripeStartIndex; // actual starting index of stripe
  int stripeWidth;
  int actualDataWidth;  // actual length of data buffers

  protected ECChunk[] decodeInputs;

  StripeReader(AlignedStripe alignedStripe,
      ErasureCodingPolicy ecPolicy, LocatedBlock[] targetBlocks,
      BlockReaderInfo[] readerInfos, CorruptedBlocks corruptedBlocks,
      RawErasureDecoder decoder,
      DFSStripedInputStream dfsStripedInputStream) {
    this.alignedStripe = alignedStripe;
    this.ecPolicy = ecPolicy;
    this.dataBlkNum = (short)ecPolicy.getNumDataUnits();
    this.parityBlkNum = (short)ecPolicy.getNumParityUnits();
    this.cellSize = ecPolicy.getCellSize();
    this.targetBlocks = targetBlocks;
    this.readerInfos = readerInfos;
    this.corruptedBlocks = corruptedBlocks;
    this.decoder = decoder;
    this.dfsStripedInputStream = dfsStripedInputStream;

    // defaults
    this.stripeStartIndex = 0;
    this.stripeWidth = dataBlkNum;
    this.actualDataWidth = dataBlkNum;

    service = new ExecutorCompletionService<>(
            dfsStripedInputStream.getStripedReadsThreadPool());
  }

  /**
   * Prepare all the data chunks.
   */
  abstract void prepareDecodeInputs();

  /**
   * Prepare the parity chunk and block reader if necessary.
   */
  abstract boolean prepareParityChunk(int index);

  /**
   * Decode to get the missing data.
   * @throws IOException if the decoder is closed.
   */
  abstract void decode() throws IOException;

  /*
   * Default close do nothing.
   */
  void close() {
  }

  void updateState4SuccessRead(StripingChunkReadResult result) {
    Preconditions.checkArgument(
        result.state == StripingChunkReadResult.SUCCESSFUL);
    readerInfos[result.index].setOffset(alignedStripe.getOffsetInBlock()
        + alignedStripe.getSpanInBlock());
  }

  private void checkMissingBlocks() throws IOException {
    if (alignedStripe.missingChunksNum > parityBlkNum) {
      clearFutures();
      throw new IOException(alignedStripe.missingChunksNum
          + " missing blocks, the stripe is: " + alignedStripe
          + "; locatedBlocks is: " + dfsStripedInputStream.getLocatedBlocks());
    }
  }

  /**
   * We need decoding. Thus go through all the data chunks and make sure we
   * submit read requests for all of them.
   */
  private void readDataForDecoding() throws IOException {
    prepareDecodeInputs();
    for (int i = 0; i < dataBlkNum; i++) {
      Preconditions.checkNotNull(alignedStripe.chunks[i]);
      if (alignedStripe.chunks[i].state == StripingChunk.REQUESTED) {
        if (!readChunk(targetBlocks[i], i)) {
          alignedStripe.missingChunksNum++;
        }
      }
    }
    checkMissingBlocks();
  }

  void readParityChunks(int num) throws IOException {
    for (int i = actualDataWidth, j = 0; i < actualDataWidth + parityBlkNum && j < num;
         i++) {
      if (alignedStripe.chunks[i] == null) {
        if (prepareParityChunk(i - actualDataWidth) && readChunk(targetBlocks[i], i)) {
          j++;
        } else {
          alignedStripe.missingChunksNum++;
        }
      }
    }
    checkMissingBlocks();
  }

  private ByteBufferStrategy[] getReadStrategies(StripingChunk chunk) {
    if (chunk.useByteBuffer()) {
      ByteBufferStrategy strategy = new ByteBufferStrategy(
          chunk.getByteBuffer(), dfsStripedInputStream.getReadStatistics(),
          dfsStripedInputStream.getDFSClient());
      return new ByteBufferStrategy[]{strategy};
    }

    ByteBufferStrategy[] strategies =
        new ByteBufferStrategy[chunk.getChunkBuffer().getSlices().size()];
    for (int i = 0; i < strategies.length; i++) {
      ByteBuffer buffer = chunk.getChunkBuffer().getSlice(i);
      strategies[i] = new ByteBufferStrategy(buffer,
              dfsStripedInputStream.getReadStatistics(),
              dfsStripedInputStream.getDFSClient());
    }
    return strategies;
  }

  private int readToBuffer(BlockReader blockReader,
      DatanodeInfo currentNode, ByteBufferStrategy strategy,
      ExtendedBlock currentBlock) throws IOException {
    final int targetLength = strategy.getTargetLength();
    int length = 0;
    try {
      while (length < targetLength) {
        int ret = strategy.readFromBlock(blockReader);
        if (ret < 0) {
          throw new IOException("Unexpected EOS from the reader");
        }
        length += ret;
      }
      return length;
    } catch (ChecksumException ce) {
      DFSClient.LOG.warn("Found Checksum error for "
          + currentBlock + " from " + currentNode
          + " at " + ce.getPos());
      //Clear buffer to make next decode success
      strategy.getReadBuffer().clear();
      // we want to remember which block replicas we have tried
      corruptedBlocks.addCorruptedBlock(currentBlock, currentNode);
      throw ce;
    } catch (IOException e) {
      DFSClient.LOG.warn("Exception while reading from "
          + currentBlock + " of " + dfsStripedInputStream.getSrc() + " from "
          + currentNode, e);
      //Clear buffer to make next decode success
      strategy.getReadBuffer().clear();
      throw e;
    }
  }

  private Callable<BlockReadStats> readCells(final BlockReader reader,
      final DatanodeInfo datanode, final long currentReaderOffset,
      final long targetReaderOffset, final ByteBufferStrategy[] strategies,
      final ExtendedBlock currentBlock) {
    return () -> {
      // reader can be null if getBlockReaderWithRetry failed or
      // the reader hit exception before
      if (reader == null) {
        throw new IOException("The BlockReader is null. " +
            "The BlockReader creation failed or the reader hit exception.");
      }
      Preconditions.checkState(currentReaderOffset <= targetReaderOffset);
      if (currentReaderOffset < targetReaderOffset) {
        long skipped = reader.skip(targetReaderOffset - currentReaderOffset);
        Preconditions.checkState(
            skipped == targetReaderOffset - currentReaderOffset);
      }

      int ret = 0;
      for (ByteBufferStrategy strategy : strategies) {
        int bytesReead = readToBuffer(reader, datanode, strategy, currentBlock);
        ret += bytesReead;
      }
      return new BlockReadStats(ret, reader.isShortCircuit(),
          reader.getNetworkDistance());
    };
  }

  boolean readChunk(final LocatedBlock block, int chunkIndex)
      throws IOException {
    final StripingChunk chunk = alignedStripe.chunks[chunkIndex];
    if (block == null) {
      chunk.state = StripingChunk.MISSING;
      return false;
    }

    if (readerInfos[chunkIndex] == null) {
      if (!dfsStripedInputStream.createBlockReader(block,
          alignedStripe.getOffsetInBlock(), readTo,
          targetBlocks, readerInfos, chunkIndex)) {
        chunk.state = StripingChunk.MISSING;
        return false;
      }
    } else if (readerInfos[chunkIndex].shouldSkip) {
      chunk.state = StripingChunk.MISSING;
      return false;
    }

    chunk.state = StripingChunk.PENDING;
    Callable<BlockReadStats> readCallable =
        readCells(readerInfos[chunkIndex].reader,
        readerInfos[chunkIndex].datanode,
        readerInfos[chunkIndex].blockReaderOffset,
        alignedStripe.getOffsetInBlock(), getReadStrategies(chunk),
        block.getBlock());

    Future<BlockReadStats> request = service.submit(readCallable);
    futures.put(request, chunkIndex);
    return true;
  }

  /**
   * read the whole stripe. do decoding if necessary
   */
  void readStripe() throws IOException {
    for (int i = stripeStartIndex; i < stripeStartIndex + stripeWidth; i++) {
      if (alignedStripe.chunks[i] != null &&
          alignedStripe.chunks[i].state != StripingChunk.ALLZERO) {
        if (!readChunk(targetBlocks[i], i)) {
          failedNodes.add(targetBlocks[i].getLocations()[0]);
          alignedStripe.missingChunksNum++;
        }
      }
    }
    // There are missing block locations at this stage. Thus we need to read
    // the full stripe and one more parity block.
    if (alignedStripe.missingChunksNum > 0) {
      degradedRead = true;
      checkMissingBlocks();
      // read all the data in the stripe/group (it's possible it's already read)
      readDataForDecoding();
      // read parity chunks
      readParityChunks(alignedStripe.missingChunksNum);
    }

    // Input buffers for potential decode operation, which remains null until
    // first read failure
    final long start = System.currentTimeMillis();
    while (!futures.isEmpty()) {
      try {
        StripingChunkReadResult r = StripedBlockUtil
            .getNextCompletedStripedRead(service, futures, 0);
        dfsStripedInputStream.updateReadStats(r.getReadStats());
        if (DFSClient.LOG.isDebugEnabled()) {
          DFSClient.LOG.debug("Read task returned: " + r + ", for stripe "
              + alignedStripe);
        }
        StripingChunk returnedChunk = alignedStripe.chunks[r.index];
        Preconditions.checkNotNull(returnedChunk);
        Preconditions.checkState(returnedChunk.state == StripingChunk.PENDING);

        if (r.state == StripingChunkReadResult.SUCCESSFUL) {
          returnedChunk.state = StripingChunk.FETCHED;
          alignedStripe.fetchedChunksNum++;
          updateState4SuccessRead(r);
          if (alignedStripe.fetchedChunksNum == actualDataWidth) {
            clearFutures();
            break;
          }
        } else {
          returnedChunk.state = StripingChunk.MISSING;
          // close the corresponding reader
          dfsStripedInputStream.closeReader(readerInfos[r.index]);

          final int missing = alignedStripe.missingChunksNum;
          alignedStripe.missingChunksNum++;
          checkMissingBlocks();

          readDataForDecoding();
          readParityChunks(alignedStripe.missingChunksNum - missing);
        }
      } catch (InterruptedException ie) {
        DFSClient.LOG.trace("Exception in {} ms", System.currentTimeMillis() - start);
        String err = "Read request interrupted";
        DFSClient.LOG.error(err);
        clearFutures();
        // Don't decode if read interrupted
        throw new InterruptedIOException(err);
      }
    }
    final long read = System.currentTimeMillis();
    if (alignedStripe.missingChunksNum > 0) {
      DFSClient.LOG.info("Done reading {} blocks in {} ms", alignedStripe.fetchedChunksNum, read - start);
      missingUnits = alignedStripe.missingChunksNum;
      decode();
      DFSClient.LOG.info("Done decoding {} missing blocks in {} ms", missingUnits, System.currentTimeMillis() - read);
    }
  }

  /**
   * Some fetched {@link StripingChunk} might be stored in original application
   * buffer instead of prepared decode input buffers. Some others are beyond
   * the range of the internal blocks and should correspond to all zero bytes.
   * When all pending requests have returned, this method should be called to
   * finalize decode input buffers.
   */

  void finalizeDecodeInputs() {
    // fill data portion of EC group, for now default to using the first stripe
    // makes no difference in terms of performance as both stripes are already buffered
    for (int i = 0; i < dataBlkNum; i++) {
      final StripingChunk chunk = alignedStripe.chunks[i];
      if (chunk != null && chunk.state == StripingChunk.FETCHED) {
        if (chunk.useChunkBuffer()) {
          chunk.getChunkBuffer().copyTo(decodeInputs[i].getBuffer());
        } else {
          chunk.getByteBuffer().flip();
        }
      } else if (chunk != null && chunk.state == StripingChunk.ALLZERO) {
        decodeInputs[i].setAllZero(true);
      }
    }

    // fill parities of the EC group using the same loop
    for (int i = 0; i < parityBlkNum; i++) {
      final StripingChunk chunk = alignedStripe.chunks[actualDataWidth + i];
      if (chunk != null && chunk.state == StripingChunk.FETCHED) {
        if (chunk.useChunkBuffer()) {
          chunk.getChunkBuffer().copyTo(decodeInputs[dataBlkNum + i].getBuffer());
        } else {
          chunk.getByteBuffer().flip();
        }
      } else if (chunk != null && chunk.state == StripingChunk.ALLZERO) {
        decodeInputs[dataBlkNum + i].setAllZero(true);
      }
    }
  }

  /**
   * Decode based on the given input buffers and erasure coding policy.
   */
  void decodeAndFillBuffer(boolean fillBuffer) throws IOException {
    // Step 1: prepare indices and output buffers for missing data units
    int[] decodeIndices = prepareErasedIndices();
    int[] pseudoIndices = new int[decodeIndices.length];
    for (int i = 0; i < decodeIndices.length; i++) {
      pseudoIndices[i] = decodeIndices[i] % dataBlkNum;
    }

    final int decodeChunkNum = decodeIndices.length;
    ECChunk[] outputs = new ECChunk[decodeChunkNum];
    for (int i = 0; i < decodeChunkNum; i++) {
      outputs[i] = decodeInputs[pseudoIndices[i]];
      decodeInputs[pseudoIndices[i]] = null;
    }

    long start = Time.monotonicNow();
    // Step 2: decode into prepared output buffers
    decoder.decode(decodeInputs, pseudoIndices, outputs);

    // Step 3: fill original application buffer with decoded data
    if (fillBuffer) {
      for (int i = 0; i < decodeIndices.length; i++) {
        int missingBlkIdx = decodeIndices[i];
        StripingChunk chunk = alignedStripe.chunks[missingBlkIdx];
        if (chunk.state == StripingChunk.MISSING && chunk.useChunkBuffer()) {
          chunk.getChunkBuffer().copyFrom(outputs[i].getBuffer());
        }
      }
    }
    long end = Time.monotonicNow();
    // Decoding time includes CPU time on erasure coding and memory copying of
    // decoded data.
    dfsStripedInputStream.readStatistics.addErasureCodingDecodingTime(
        end - start);
  }

  /**
   * Prepare erased indices.
   */
  int[] prepareErasedIndices() {
    int[] decodeIndices = new int[parityBlkNum];
    int pos = 0;
    for (int i = 0; i < alignedStripe.chunks.length; i++) {
      if (alignedStripe.chunks[i] != null &&
          alignedStripe.chunks[i].state == StripingChunk.MISSING){
        decodeIndices[pos++] = i;
      }
    }

    int[] erasedIndices = Arrays.copyOf(decodeIndices, pos);
    return erasedIndices;
  }

  void clearFutures() {
    for (Future future : futures.keySet()) {
      future.cancel(false);
    }
    futures.clear();
  }

  boolean useDirectBuffer() {
    return decoder.preferDirectBuffer();
  }
}
