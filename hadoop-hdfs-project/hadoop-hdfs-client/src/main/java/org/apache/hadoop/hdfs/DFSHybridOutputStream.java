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

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.DatanodeInfoBuilder;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Time;
import org.apache.htrace.core.TraceScope;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


/**
 * This class supports writing files in striped layout and erasure coded format.
 * Each stripe contains a sequence of cells.
 */
@InterfaceAudience.Private
public class DFSHybridOutputStream extends DFSOutputStream
        implements StreamCapabilities {
  private static final ByteBufferPool BUFFER_POOL = new ElasticByteBufferPool();

  /**
   * OutputStream level last exception, will be used to indicate the fatal
   * exception of this stream, i.e., being aborted.
   */
  private final ExceptionLastSeen exceptionLastSeen = new ExceptionLastSeen();

  /** Coordinate the communication between the streamers. */
  public static class HybridCoordinator extends Coordinator  {

    /**
     * The set of nodes on which to stripe data across, not initially sent to but forwarded
     */
    private final List<DatanodeInfoWithStorage> stripeNodes;

    private ExtendedBlock stripeBlock;

    private ExtendedBlock parityBlock;

    private final ErasureCodingPolicy ecPolicy;

    public HybridCoordinator(final int numAllBlocks, final ErasureCodingPolicy ecPolicy) {
      super(numAllBlocks);
      this.stripeNodes = new ArrayList<>();
      this.stripeBlock = null;
      this.parityBlock = null;
      this.ecPolicy = ecPolicy;
    }

    void addStripeNode(DatanodeInfoWithStorage node) {
      stripeNodes.add(node);
    }

    DatanodeInfoWithStorage getStripeNodeAt(int index) {
      return stripeNodes.get(index);
    }

    void setStripeBlock(ExtendedBlock stblock) {
      stripeBlock = stblock;
    }

    void setParityBlock(ExtendedBlock ptblock) {
      parityBlock = ptblock;
    }

    void addNumBytes(long numBytes) {
      stripeBlock.setNumBytes(stripeBlock.getNumBytes() + numBytes);
    }

    DatanodeInfo[] getStripeNodes() {
      return stripeNodes.toArray(DatanodeInfo.EMPTY_ARRAY);
    }

    ExtendedBlock getStripeBlock() {
      return stripeBlock;
    }

    ExtendedBlock getParityBlock() {
      return parityBlock;
    }

    StorageType[] getStripeNodeStorageTypes() {
      StorageType[] types = new StorageType[stripeNodes.size()];
      for (int i = 0; i < stripeNodes.size(); i++) {
        types[i] = stripeNodes.get(i).getStorageType();
      }
      return types;
    }

    String[] getStripeNodeStorageIDs() {
      String[] ids = new String[stripeNodes.size()];
      for (int i = 0; i < stripeNodes.size(); i++) {
        ids[i] = stripeNodes.get(i).getStorageID();
      }
      return ids;
    }

    ErasureCodingPolicy getEcPolicy() {
      return ecPolicy;
    }

    void clearStripeNodes() {
      stripeNodes.clear();
    }

  }

  /** Buffers for writing the data and parity cells of a stripe. */
  class CellBuffers {
    private final ByteBuffer[] buffers;
    private final byte[][] checksumArrays;

    CellBuffers() {
      if (cellSize % bytesPerChecksum != 0) {
        throw new HadoopIllegalArgumentException("Invalid values: "
                + HdfsClientConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY + " (="
                + bytesPerChecksum + ") must divide cell size (=" + cellSize + ").");
      }

      checksumArrays = new byte[numParityBlocks][];
      final int size = getChecksumSize() * (cellSize / bytesPerChecksum);
      for (int i = 0; i < checksumArrays.length; i++) {
        checksumArrays[i] = new byte[size];
      }

      buffers = new ByteBuffer[numDataBlocks + numParityBlocks];
      for (int i = 0; i < buffers.length; i++) {
        buffers[i] = BUFFER_POOL.getBuffer(useDirectBuffer(), cellSize);
        buffers[i].limit(cellSize);
      }
    }

    private ByteBuffer[] getBuffers() {
      return buffers;
    }

    byte[] getChecksumArray(int i) {
      return checksumArrays[i - numDataBlocks];
    }

    private int addTo(int i, byte[] b, int off, int len) {
      final ByteBuffer buf = buffers[i];
      final int pos = buf.position() + len;
      Preconditions.checkState(pos <= cellSize);
      buf.put(b, off, len);
      return pos;
    }

    private void clear() {
      for (int i = 0; i< numDataBlocks + numParityBlocks; i++) {
        buffers[i].clear();
        buffers[i].limit(cellSize);
      }
    }

    private void release() {
      for (int i = 0; i < numDataBlocks + numParityBlocks; i++) {
        if (buffers[i] != null) {
          BUFFER_POOL.putBuffer(buffers[i]);
          buffers[i] = null;
        }
      }
    }

    private void flipDataBuffers() {
      for (int i = 0; i < numDataBlocks; i++) {
        buffers[i].flip();
      }
    }
  }

  private final HybridCoordinator coordinator;
  private final CellBuffers cellBuffers;
  private final ErasureCodingPolicy ecPolicy;
  private final RawErasureEncoder encoder;
  private final List<HybridDataStreamer> streamers;
  private final List<StripedDataStreamer> pstreamers;
  private final DFSPacket[] currentPackets; // current Packet of each streamer
  private final DFSPacket[] parityPackets;

  // Size of each striping cell, must be a multiple of bytesPerChecksum.
  private final int numAllBlocks;
  private final int numDataBlocks;
  private int numParityBlocks;
  private int cellBufferIndex;
  private int currentBlockSize;
  private ExtendedBlock currentBlockGroup;
  private ExtendedBlock currentParityGroup;
  private ExtendedBlock currentReplicaSet;
  private String[] favoredNodes;
  private final List<HybridDataStreamer> failedStreamers;
  private final Map<Integer, Integer> corruptBlockCountMap;
  private ExecutorService flushAllExecutor;
  private CompletionService<Void> flushAllExecutorCompletionService;
  private int blockGroupIndex;
  private long datanodeRestartTimeout;
  private boolean paritiesEnabled = true;
  private boolean pushParityComputationToDatanodes = false;
  private int secretNumParities;
  public static DatanodeInfoWithStorage datanodeToKill;
  public static DatanodeInfoWithStorage datanodeToKill2;
  public static DatanodeInfoWithStorage datanodeToKill3;

  // everything below is for metrics collections
  private long totalInitTime = 0L;
  private long totalGroupAllocTime = 0L;
  private long totalReplicaAllocTime = 0L;
  private long totalDataWriteTime = 0L;
  private long totalParityWriteTime = 0L;
  private long totalWaitingTime;
  private long totalEncodingTime = 0L;
  private long totalCloseTime = 0L;
  private long globalStartTime;
  private long stripeStartTime;

  /** Construct a new output stream for creating a file. */
  DFSHybridOutputStream(DFSClient dfsClient, String src, HdfsFileStatus stat,
                         EnumSet<CreateFlag> flag, Progressable progress,
                         DataChecksum checksum, String[] favoredNodes) {
    super(dfsClient, src, stat, flag, progress, checksum, favoredNodes, false);
    globalStartTime = System.currentTimeMillis();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating DFSHybridOutputStream for " + src);
    }

    ecPolicy = stat.getErasureCodingPolicy();
    cellSize = ecPolicy.getCellSize();
    numDataBlocks = ecPolicy.getNumDataUnits();
    numParityBlocks = ecPolicy.getNumParityUnits();

    // hack to disable parities for writes easily
    int hack = dfsClient.getConf().getMaxRetryAttempts();
    if (hack < 10) {
      paritiesEnabled = false;
      if (hack == 8) {
        LOG.info("Pushing parity computation to datanodes");
        pushParityComputationToDatanodes = true;
        // we do this so we remember the actual number of parity units
        // but the rest of the class thinks there aren't any parities here
        secretNumParities = numParityBlocks;
      } else {
        LOG.info("Parities completely disabled for hybrid write");
      }
      numParityBlocks = 0;
    }

    // TODO: this is actually not correct, only correct when replica size = ec block size
    // numAllBlocks = numReplicaBlocks + numParityBlocks where numReplicaBlocks = k * replica size / ec block size
    // numAllBlocks is effectively just the number of immediate block connections, there are more blocks
    // that are eventually mirrored to when writing to DNs that aren't calculated here
    numAllBlocks = numDataBlocks + numParityBlocks;

    this.favoredNodes = favoredNodes;
    failedStreamers = new ArrayList<>();
    corruptBlockCountMap = new LinkedHashMap<>();
    flushAllExecutor = Executors.newFixedThreadPool(numAllBlocks);
    flushAllExecutorCompletionService = new
            ExecutorCompletionService<>(flushAllExecutor);

    ErasureCoderOptions coderOptions = new ErasureCoderOptions(
            numDataBlocks, numParityBlocks);
    encoder = CodecUtil.createRawEncoder(dfsClient.getConfiguration(),
            ecPolicy.getCodecName(), coderOptions);

    coordinator = new HybridCoordinator(numAllBlocks, ecPolicy);
    cellBuffers = new CellBuffers();

    streamers = new ArrayList<>(numDataBlocks);
    pstreamers = new ArrayList<>(numParityBlocks);
    for (short i = 0; i < numAllBlocks; i++) {
      if (i < numDataBlocks) {
        streamers.add(new HybridDataStreamer(stat,
            dfsClient, src, progress, checksum, cachingStrategy, byteArrayManager,
            favoredNodes, i, coordinator, getAddBlockFlags()));
      } else {
        pstreamers.add(new StripedDataStreamer(stat,
            dfsClient, src, progress, checksum, cachingStrategy, byteArrayManager,
            favoredNodes, i, coordinator, getAddBlockFlags()));
      }
    }
    currentPackets = new DFSPacket[streamers.size()];
    parityPackets = new DFSPacket[pstreamers.size()];

    // state used for hybrid writes specifically
    cellBufferIndex = 0;
    currentBlockSize = 0;

    datanodeRestartTimeout = dfsClient.getConf().getDatanodeRestartTimeout();
    setCurrentStreamer(0);

    totalInitTime = (System.currentTimeMillis() - globalStartTime);
  }

  private boolean useDirectBuffer() {
    return encoder.preferDirectBuffer();
  }

  HybridDataStreamer getHybridDataStreamer(int i) {
    return streamers.get(i);
  }

  StripedDataStreamer getHybridParityStreamer(int i) {
    return pstreamers.get(i);
  }

  int getCurrentIndex() {
    return getCurrentStreamer().getIndex();
  }

  int getParityIndex() {
    return getCurrentParityStreamer().getIndex();
  }

  private synchronized HybridDataStreamer getCurrentStreamer() {
    return (HybridDataStreamer) streamer;
  }

  private synchronized HybridDataStreamer setCurrentStreamer(int newIdx) {
    // backup currentPacket for current streamer
    if (streamer != null) {
      int oldIdx = streamers.indexOf(getCurrentStreamer());
      if (oldIdx >= 0) {
        currentPackets[oldIdx] = currentPacket;
      }
    }

    streamer = getHybridDataStreamer(newIdx);
    currentPacket = currentPackets[newIdx];
    adjustChunkBoundary(streamer);

    return getCurrentStreamer();
  }

  private StripedDataStreamer getCurrentParityStreamer() {
    return (StripedDataStreamer) pstreamer;
  }

  private StripedDataStreamer setParityStreamer(int newIdx) {
    // backup currentPacket for current streamer
    if (pstreamer != null) {
      int oldIdx = pstreamers.indexOf(getCurrentParityStreamer());
      if (oldIdx >= 0) {
        parityPackets[oldIdx] = parityPacket;
      }
    }

    pstreamer = getHybridParityStreamer(newIdx);
    parityPacket = parityPackets[newIdx];
    adjustChunkBoundary(pstreamer);

    return getCurrentParityStreamer();
  }

  /**
   * Encode the buffers, i.e. compute parities.
   *
   * @param buffers data buffers + parity buffers
   */
  private static void encode(RawErasureEncoder encoder, int numData,
                             ByteBuffer[] buffers) throws IOException {
    final ByteBuffer[] dataBuffers = new ByteBuffer[numData];
    final ByteBuffer[] parityBuffers = new ByteBuffer[buffers.length - numData];
    System.arraycopy(buffers, 0, dataBuffers, 0, dataBuffers.length);
    System.arraycopy(buffers, numData, parityBuffers, 0, parityBuffers.length);

    encoder.encode(dataBuffers, parityBuffers);
  }

  /**
   * check all the existing HybridDataStreamer and find newly failed streamers.
   * @return The newly failed streamers.
   * @throws IOException if less than {@link #numDataBlocks} streamers are still
   *                     healthy.
   */
  private Set<HybridDataStreamer> checkStreamers() throws IOException {
    Set<HybridDataStreamer> newFailed = new HashSet<>();
    for(HybridDataStreamer s : streamers) {
      if (!s.isHealthy() && !failedStreamers.contains(s)) {
        newFailed.add(s);
      }
    }

    final int failCount = failedStreamers.size() + newFailed.size();
    if (LOG.isDebugEnabled()) {
      LOG.debug("checkStreamers: " + streamers);
      LOG.debug("healthy streamer count=" + (numAllBlocks - failCount));
      LOG.debug("original failed streamers: " + failedStreamers);
      LOG.debug("newly failed streamers: " + newFailed);
    }
    if (failCount > (numAllBlocks - numDataBlocks)) {
      closeAllStreamers();
      throw new IOException("Failed: the number of failed blocks = "
              + failCount + " > the number of parity blocks = "
              + (numAllBlocks - numDataBlocks));
    }
    return newFailed;
  }

  private void closeAllStreamers() {
    // The write has failed, Close all the streamers.
    for (HybridDataStreamer streamer : streamers) {
      streamer.close(true);
    }
  }

  private void handleCurrentStreamerFailure(String err, Exception e)
          throws IOException {
    currentPacket = null;
    handleStreamerFailure(err, e, getCurrentStreamer());
  }

  private void handleStreamerFailure(String err, Exception e,
                                     HybridDataStreamer streamer) throws IOException {
    LOG.warn("Failed: " + err + ", " + this, e);
    streamer.getErrorState().setInternalError();
    streamer.close(true);
    checkStreamers();
    currentPackets[streamer.getIndex()] = null;
  }

  private void replaceFailedStreamers() {
    assert streamers.size() == numDataBlocks;
    final int currentIndex = getCurrentIndex();
    assert currentIndex == 0;
    for (short i = 0; i < numDataBlocks; i++) {
      final HybridDataStreamer oldStreamer = getHybridDataStreamer(i);
      if (!oldStreamer.isHealthy()) {
        LOG.info("replacing previously failed streamer " + oldStreamer);
        HybridDataStreamer streamer = new HybridDataStreamer(oldStreamer.stat,
                dfsClient, src, oldStreamer.progress,
                oldStreamer.checksum4WriteBlock, cachingStrategy, byteArrayManager,
                favoredNodes, i, coordinator, getAddBlockFlags());
        streamers.set(i, streamer);
        currentPackets[i] = null;
        if (i == currentIndex) {
          this.streamer = streamer;
          this.currentPacket = null;
        }
        streamer.start();
      }
    }
  }

  private void waitEndBlocks(int i) throws IOException {
    while (getHybridDataStreamer(i).isHealthy()) {
      final ExtendedBlock b = coordinator.endBlocks.takeWithTimeout(i);
      if (b != null) {
        // ensure the size we provided is accurate
        assert b.getNumBytes() == currentBlockSize;
        // no check here because currentBlockGroup is the stripe while b is the replica set
        // StripedBlockUtil.checkBlocks(currentBlockGroup, i, b);
        return;
      }
    }
  }

  private void waitEndParityBlocks(int i) throws IOException {
    while (getHybridParityStreamer(i).isHealthy()) {
      final ExtendedBlock b = coordinator.endBlocks.takeWithTimeout(i + numDataBlocks);
      if (b != null) {
        return;
      }
    }
  }

  private DatanodeInfo[] getExcludedNodes() {
    List<DatanodeInfo> excluded = new ArrayList<>();
    for (HybridDataStreamer streamer : streamers) {
      for (DatanodeInfo e : streamer.getExcludedNodes()) {
        if (e != null) {
          excluded.add(e);
        }
      }
    }
    return excluded.toArray(new DatanodeInfo[excluded.size()]);
  }

  private void allocateNewReplicaSet() throws IOException {
    long start = System.currentTimeMillis();

    // to async allocate new block, make copy and set to current size
    ExtendedBlock currBlock = new ExtendedBlock(currentReplicaSet);
    currBlock.setNumBytes(currentBlockSize);

    // allocate a new block
    final LocatedBlock lb;
    DatanodeInfo[] excluded = getExcludedNodes();
    try {
      lb = addBlock(excluded, dfsClient, src, currBlock,
          fileId, favoredNodes, getAddBlockFlags());
    } catch (IOException ioe) {
      closeAllStreamers();
      throw ioe;
    }

    // should be a replicated block
    assert !lb.isStriped();

    // push onto next block queue the new replica
    coordinator.getFollowingBlocks().offer(0, lb);
    coordinator.addNumBytes(blockSize);

    // we need to close out the streamer/wait for block to finish
    if (currentBlockGroup != null) {
      waitEndBlocks(0);
    }

    // add tracking vars
    currentReplicaSet = lb.getBlock();
    currentBlockSize = 0;
    totalReplicaAllocTime += (System.currentTimeMillis() - start);
  }

  private void allocateNewHybridGroup() throws IOException {
    stripeStartTime = System.currentTimeMillis();

    // replace failed streamers
    failedStreamers.clear();
    DatanodeInfo[] excludedNodes = getExcludedNodes();
    LOG.debug("Excluding DataNodes when allocating new block: "
            + Arrays.asList(excludedNodes));
    replaceFailedStreamers();

    // Ask namenode for new blocks
    ExtendedBlock prevBlockGroup = currentBlockGroup;
    ExtendedBlock prevParityGroup = currentParityGroup;
    if (prevParityGroup != null) {
      prevParityGroup.setNumBytes((numParityBlocks + secretNumParities) * StripedBlockUtil.getInternalBlockLength(
          currentBlockGroup.getNumBytes(), cellSize, numDataBlocks, 0));
      // replace favored nodes since favored nodes may have same striper
      if (pushParityComputationToDatanodes) {
        favoredNodes = new String[secretNumParities];
        for (int i = 0; i < secretNumParities; i++) {
          favoredNodes[i] = coordinator.getStripeNodeAt(numDataBlocks + i).getXferAddr();
        }
      } else if (paritiesEnabled) {
        // TODO: test this code path
        favoredNodes = new String[numParityBlocks];
        for (int i = 0; i < numParityBlocks; i++) {
          favoredNodes[i] = getHybridParityStreamer(i).getNode().getXferAddr();
        }
      }
    }
    ExtendedBlock prevReplicaSet = currentReplicaSet;
    if (prevReplicaSet != null) {
      prevReplicaSet.setNumBytes(currentBlockSize);
    }

    LOG.debug("Allocating new block group. The previous block group: "
            + prevBlockGroup);
    final LocatedBlock lb;
    try {
      lb = addHybridBlock(excludedNodes, dfsClient, src,
              prevBlockGroup, prevParityGroup, prevReplicaSet,
          paritiesEnabled || pushParityComputationToDatanodes,
          fileId, favoredNodes, getAddBlockFlags());
    } catch (IOException ioe) {
      closeAllStreamers();
      throw ioe;
    }
    assert lb.isHybrid();

    // wait for previous blocks to be ack'ed
    if (currentBlockGroup != null) {
      waitEndBlocks(0);
    }

    if (currentReplicaSet != null && numParityBlocks > 0) {
      for (int p = 0; p < numParityBlocks; p++) {
        waitEndParityBlocks(p);
      }
    }

//    datanodeToKill = datanodeToKill != null ? datanodeToKill : lb.getLocations()[0];
//    datanodeToKill2 = datanodeToKill2 != null ? datanodeToKill2 : lb.getLocations()[9];
//    datanodeToKill3 = datanodeToKill3 != null ? datanodeToKill3 : lb.getLocations()[10];

    // parse out fields from namenode
    parseHybridBlockGroupIntoCoordinator((LocatedHybridBlock) lb);
    totalGroupAllocTime += (System.currentTimeMillis() - stripeStartTime);
  }

  private void parseHybridBlockGroupIntoCoordinator(LocatedHybridBlock bg) {
    // coordinator tracks the main stripe block which has the same
    // id as the hybrid block itself
    coordinator.setStripeBlock(new ExtendedBlock(bg.getBlock()));

    // assign the new block to the current block group
    currentBlockGroup = bg.getBlock();
    currentParityGroup = bg.getParity();
    blockGroupIndex++;

    if (pushParityComputationToDatanodes) {
      numParityBlocks = secretNumParities;
    }

    // TODO: modify this class to have one set of packets and one data streamer
    //  that is only streaming to one place, this streamer changes when get
    //  additional block is called
    // TODO: or for now, just return as usual?
    coordinator.clearStripeNodes();

    // parse the new blocks such that:
    // first k are striped data nodes
    // next p are striped parity nodes
    // all nodes after k + p are replicated nodes in the order of
    // 1-8A, 1-8B, 9-16A, 9-16B, 17-24A, 17-24B, ...
    int numTotalNodes = bg.getLocations().length;
    int i, j, pos;
    for (i = 0; i < numDataBlocks; i++) {
      // first k are striping data nodes, these are added to coordinator.stripeNodes
      coordinator.addStripeNode(bg.getLocations()[i]);
    }

    for (i = numDataBlocks + numParityBlocks; i < numTotalNodes; i += blockReplication) {
      // increment by block repl
      pos = i - (numDataBlocks + numParityBlocks);

      ExtendedBlock block = new ExtendedBlock(bg.getReplicas()[pos]);

      DatanodeInfo[] locations = new DatanodeInfo[blockReplication];
      String[] storageIds = new String[blockReplication];
      StorageType[] storageTypes = new StorageType[blockReplication];

      // initialize and then iterate from i to block repl
      for (j = 0; j < blockReplication; j++) {
        locations[j] = bg.getLocations()[i + j];
        storageIds[j] = bg.getStorageIDs()[i + j];
        storageTypes[j] = bg.getStorageTypes()[i + j];
      }

      LocatedBlock lb = new LocatedBlock(block,
              locations, storageIds, storageTypes,
              bg.getStartOffset(), bg.isCorrupt(), null);
      coordinator.getFollowingBlocks().offer(pos / blockReplication, lb);
      currentReplicaSet = lb.getBlock();
      currentBlockSize = 0;
    }

    for (i = numDataBlocks; i < numDataBlocks + numParityBlocks; i++) {
      // finally add the striped parity blocks to followingBlocks
      if (pushParityComputationToDatanodes) {
        // when pushing parity comp to datanodes, add these to the striped nodes
        coordinator.addStripeNode(bg.getLocations()[i]);
      } else {
        // otherwise set them as usual in the coordinator
        ExtendedBlock block = new ExtendedBlock(bg.getParity());
        block.setBlockId(bg.getParity().getBlockId() + (i - numDataBlocks));
        LocatedBlock lb = new LocatedBlock(block,
            new DatanodeInfo[]{bg.getLocations()[i]},
            new String[]{bg.getStorageIDs()[i]},
            new StorageType[]{bg.getStorageTypes()[i]},
            bg.getStartOffset(), bg.isCorrupt(), null);
        coordinator.getFollowingBlocks().offer(i, lb);
      }
    }

    if (pushParityComputationToDatanodes) {
      // set parity
      coordinator.setParityBlock(bg.getParity());

      // use favored nodes to get namenode to allocate to that node on the next rotation
      // choose the replica set's striper node
      favoredNodes = new String[]{bg.getLocations()[numDataBlocks + numParityBlocks + blockReplication - 1].getXferAddr()};

      // reset
      numParityBlocks = 0;
    }
  }

  private boolean shouldEndBlockGroup() {
    return currentBlockGroup != null &&
            currentBlockGroup.getNumBytes() == blockSize * numDataBlocks;
  }

  @Override
  protected synchronized void writeChunk(byte[] bytes, int offset, int len,
                                         byte[] checksum, int ckoff, int cklen) throws IOException {
    // there are now two different indices (well... three)
    // 1. data streamer index to indicate which replica to forward to
    // 2. parity streamer index to indicate which parity is sending
    // 3. cell buffer index to indicate which index in cell buffer to add to

    // cellPos is the position within the cell buffer specifically, up to 1 MB
    final int cellIndex = cellBufferIndex;
    final int cellPos = cellBuffers.addTo(cellIndex, bytes, offset, len);
    // determine fullness
    final boolean cellFull = cellPos == cellSize;
    final boolean blockFull = currentBlockSize == blockSize;
    boolean collect = false;

    if (currentBlockGroup == null || shouldEndBlockGroup()) {
      // the incoming data should belong to a new hybrid group. Allocate a new group
      if (currentBlockGroup != null) {
        // gather metrics for this block group
        collect = true;
      }
      allocateNewHybridGroup();
    } else if (blockFull) {
      allocateNewReplicaSet();
    }

    // currentBlockSize is the current size of the replica we are writing to
    currentBlockSize += len;
    currentBlockGroup.setNumBytes(currentBlockGroup.getNumBytes() + len);

    // send out data on the streamer
    long start = System.nanoTime();
    super.writeChunk(bytes, offset, len, checksum, ckoff, cklen);
    totalDataWriteTime += (System.nanoTime() - start);

    // When the cell is full, need to push cell buffer index forward
    if (cellFull) {
      int next = cellIndex + 1;
      // Possible you've reached the end of a stripe, in which case
      // compute and send out parities for that stripe, reset cellBuffer
      if (next == numDataBlocks) {
        next = 0;
        cellBuffers.flipDataBuffers();
        if (paritiesEnabled) {
          writeParityCells();
        }
        cellBuffers.clear();
      }
      cellBufferIndex = next;
    }

    if (collect) {
      final long totalTimeOnStripe = System.currentTimeMillis() - stripeStartTime;
      totalWaitingTime += (totalTimeOnStripe - (totalDataWriteTime / (1000 * 1000)));
    }
  }

  @Override
  synchronized void enqueueCurrentPacketFull() throws IOException {
    LOG.debug("enqueue full {}, src={}, bytesCurBlock={}, blockSize={},"
                    + " appendChunk={}, {}", currentPacket, src, getStreamer()
                    .getBytesCurBlock(), blockSize, getStreamer().getAppendChunk(),
            getStreamer());
    enqueueCurrentPacket();
    adjustChunkBoundary(getStreamer());
    if (shouldEndBlockGroup()) {
      endBlock();
    } else {
      endReplicaBlock();
    }
  }

  /**
   * @return whether the data streamer with the given index is streaming data.
   * Note the streamer may not be in STREAMING stage if the block length is less
   * than a stripe.
   */
  private boolean isStreamerWriting(int streamerIndex) {
    final long length = currentBlockGroup == null ?
            0 : currentBlockGroup.getNumBytes();
    if (length == 0) {
      return false;
    }
    if (streamerIndex >= numDataBlocks) {
      return true;
    }
    final int numCells = (int) ((length - 1) / cellSize + 1);
    return streamerIndex < numCells;
  }

  private Set<HybridDataStreamer> markExternalErrorOnStreamers() {
    Set<HybridDataStreamer> healthySet = new HashSet<>();
    for (int i = 0; i < numDataBlocks; i++) {
      final HybridDataStreamer streamer = getHybridDataStreamer(i);
      if (streamer.isHealthy() && isStreamerWriting(i)) {
        Preconditions.checkState(
                streamer.getStage() == BlockConstructionStage.DATA_STREAMING,
                "streamer: " + streamer);
        streamer.setExternalError();
        healthySet.add(streamer);
      } else if (!streamer.streamerClosed()
              && streamer.getErrorState().hasDatanodeError()
              && streamer.getErrorState().doWaitForRestart()) {
        healthySet.add(streamer);
        failedStreamers.remove(streamer);
      }
    }
    return healthySet;
  }

  /**
   * Check and handle data streamer failures. This is called only when we have
   * written a full stripe (i.e., enqueue all packets for a full stripe), or
   * when we're closing the outputstream.
   */
  private void checkStreamerFailures(boolean isNeedFlushAllPackets)
          throws IOException {
    Set<HybridDataStreamer> newFailed = checkStreamers();
    if (newFailed.size() == 0) {
      return;
    }

    if (isNeedFlushAllPackets) {
      // for healthy streamers, wait till all of them have fetched the new block
      // and flushed out all the enqueued packets.
      flushAllInternals();
    }
    // recheck failed streamers again after the flush
    newFailed = checkStreamers();
    while (newFailed.size() > 0) {
      failedStreamers.addAll(newFailed);
      coordinator.clearFailureStates();
      corruptBlockCountMap.put(blockGroupIndex, failedStreamers.size());

      // mark all the healthy streamers as external error
      Set<HybridDataStreamer> healthySet = markExternalErrorOnStreamers();

      // we have newly failed streamers, update block for pipeline
      final ExtendedBlock newBG = updateBlockForPipeline(healthySet);

      // wait till all the healthy streamers to
      // 1) get the updated block info
      // 2) create new block outputstream
      newFailed = waitCreatingStreamers(healthySet);
      if (newFailed.size() + failedStreamers.size() >
              numAllBlocks - numDataBlocks) {
        // The write has failed, Close all the streamers.
        closeAllStreamers();
        throw new IOException(
                "Data streamers failed while creating new block streams: "
                        + newFailed + ". There are not enough healthy streamers.");
      }
      for (HybridDataStreamer failedStreamer : newFailed) {
        assert !failedStreamer.isHealthy();
      }

      // TODO we can also succeed if all the failed streamers have not taken
      // the updated block
      if (newFailed.size() == 0) {
        // reset external error state of all the streamers
        for (HybridDataStreamer streamer : healthySet) {
          assert streamer.isHealthy();
          streamer.getErrorState().reset();
        }
        updatePipeline(newBG);
      }
      for (int i = 0; i < numAllBlocks; i++) {
        coordinator.offerStreamerUpdateResult(i, newFailed.size() == 0);
      }
      //wait for get notify to failed stream
      if (newFailed.size() != 0) {
        try {
          Thread.sleep(datanodeRestartTimeout);
        } catch (InterruptedException e) {
          // Do nothing
        }
      }
    }
  }

  /**
   * Check if the streamers were successfully updated, adding failed streamers
   * in the <i>failed</i> return parameter.
   * @param failed Return parameter containing failed streamers from
   *               <i>streamers</i>.
   * @param streamers Set of streamers that are being updated
   * @return total number of successful updates and failures
   */
  private int checkStreamerUpdates(Set<HybridDataStreamer> failed,
                                   Set<HybridDataStreamer> streamers) {
    for (HybridDataStreamer streamer : streamers) {
      if (!coordinator.updateStreamerMap.containsKey(streamer)) {
        if (!streamer.isHealthy() &&
                coordinator.getNewBlocks().peek(streamer.getIndex()) != null) {
          // this streamer had internal error before getting updated block
          failed.add(streamer);
        }
      }
    }
    return coordinator.updateStreamerMap.size() + failed.size();
  }

  /**
   * Waits for streamers to be created.
   *
   * @param healthyStreamers Set of healthy streamers
   * @return Set of streamers that failed.
   *
   * @throws IOException
   */
  private Set<HybridDataStreamer> waitCreatingStreamers(
          Set<HybridDataStreamer> healthyStreamers) throws IOException {
    Set<HybridDataStreamer> failed = new HashSet<>();
    final int expectedNum = healthyStreamers.size();
    final long socketTimeout = dfsClient.getConf().getSocketTimeout();
    // the total wait time should be less than the socket timeout, otherwise
    // a slow streamer may cause other streamers to timeout. here we wait for
    // half of the socket timeout
    long remaingTime = socketTimeout > 0 ? socketTimeout/2 : Long.MAX_VALUE;
    final long waitInterval = 1000;
    synchronized (coordinator) {
      while (checkStreamerUpdates(failed, healthyStreamers) < expectedNum
              && remaingTime > 0) {
        try {
          long start = Time.monotonicNow();
          coordinator.wait(waitInterval);
          remaingTime -= Time.monotonicNow() - start;
        } catch (InterruptedException e) {
          throw DFSUtilClient.toInterruptedIOException("Interrupted when waiting" +
                  " for results of updating striped streamers", e);
        }
      }
    }
    synchronized (coordinator) {
      for (HybridDataStreamer streamer : healthyStreamers) {
        if (!coordinator.updateStreamerMap.containsKey(streamer)) {
          // close the streamer if it is too slow to create new connection
          LOG.info("close the slow stream " + streamer);
          streamer.setStreamerAsClosed();
          failed.add(streamer);
        }
      }
    }
    for (Map.Entry<DataStreamer, Boolean> entry :
            coordinator.updateStreamerMap.entrySet()) {
      if (!entry.getValue()) {
        failed.add((HybridDataStreamer) entry.getKey());
      }
    }
    for (HybridDataStreamer failedStreamer : failed) {
      healthyStreamers.remove(failedStreamer);
    }
    return failed;
  }

  /**
   * Call {@link ClientProtocol#updateBlockForPipeline} and assign updated block
   * to healthy streamers.
   * @param healthyStreamers The healthy data streamers. These streamers join
   *                         the failure handling.
   */
  private ExtendedBlock updateBlockForPipeline(
          Set<HybridDataStreamer> healthyStreamers) throws IOException {
    final LocatedBlock updated = dfsClient.namenode.updateBlockForPipeline(
            currentBlockGroup, dfsClient.clientName);
    final long newGS = updated.getBlock().getGenerationStamp();
    ExtendedBlock newBlock = new ExtendedBlock(currentBlockGroup);
    newBlock.setGenerationStamp(newGS);
    final LocatedBlock[] updatedBlks = StripedBlockUtil.parseStripedBlockGroup(
            (LocatedStripedBlock) updated, cellSize, numDataBlocks,
            numParityBlocks);

    for (int i = 0; i < numDataBlocks; i++) {
      HybridDataStreamer si = getHybridDataStreamer(i);
      if (healthyStreamers.contains(si)) {
        final LocatedBlock lb = new LocatedBlock(new ExtendedBlock(newBlock),
                null, null, null, -1, updated.isCorrupt(), null);
        lb.setBlockToken(updatedBlks[i].getBlockToken());
        coordinator.getNewBlocks().offer(i, lb);
      }
    }
    return newBlock;
  }

  private void updatePipeline(ExtendedBlock newBG) throws IOException {
    final DatanodeInfo[] newNodes = new DatanodeInfo[numAllBlocks];
    final String[] newStorageIDs = new String[numAllBlocks];
    for (int i = 0; i < numAllBlocks; i++) {
      final HybridDataStreamer streamer = getHybridDataStreamer(i);
      final DatanodeInfo[] nodes = streamer.getNodes();
      final String[] storageIDs = streamer.getStorageIDs();
      if (streamer.isHealthy() && nodes != null && storageIDs != null) {
        newNodes[i] = nodes[0];
        newStorageIDs[i] = storageIDs[0];
      } else {
        newNodes[i] = new DatanodeInfoBuilder()
                .setNodeID(DatanodeID.EMPTY_DATANODE_ID).build();
        newStorageIDs[i] = "";
      }
    }

    // Update the NameNode with the acked length of the block group
    // Save and restore the unacked length
    final long sentBytes = currentBlockGroup.getNumBytes();
    final long ackedBytes = getAckedLength();
    Preconditions.checkState(ackedBytes <= sentBytes,
            "Acked:" + ackedBytes + ", Sent:" + sentBytes);
    currentBlockGroup.setNumBytes(ackedBytes);
    newBG.setNumBytes(ackedBytes);
    dfsClient.namenode.updatePipeline(dfsClient.clientName, currentBlockGroup,
            newBG, newNodes, newStorageIDs);
    currentBlockGroup = newBG;
    currentBlockGroup.setNumBytes(sentBytes);
  }

  /**
   * Return the length of each block in the block group.
   * Unhealthy blocks have a length of -1.
   *
   * @return List of block lengths.
   */
  private List<Long> getBlockLengths() {
    List<Long> blockLengths = new ArrayList<>(numAllBlocks);
    for (int i = 0; i < numAllBlocks; i++) {
      final HybridDataStreamer streamer = getHybridDataStreamer(i);
      long numBytes = -1;
      if (streamer.isHealthy()) {
        if (streamer.getBlock() != null) {
          numBytes = streamer.getBlock().getNumBytes();
        }
      }
      blockLengths.add(numBytes);
    }
    return blockLengths;
  }

  /**
   * Get the length of acked bytes in the block group.
   *
   * <p>
   *   A full stripe is acked when at least numDataBlocks streamers have
   *   the corresponding cells of the stripe, and all previous full stripes are
   *   also acked. This enforces the constraint that there is at most one
   *   partial stripe.
   * </p>
   * <p>
   *   Partial stripes write all parity cells. Empty data cells are not written.
   *   Parity cells are the length of the longest data cell(s). For example,
   *   with RS(3,2), if we have data cells with lengths [1MB, 64KB, 0], the
   *   parity blocks will be length [1MB, 1MB].
   * </p>
   * <p>
   *   To be considered acked, a partial stripe needs at least numDataBlocks
   *   empty or written cells.
   * </p>
   * <p>
   *   Currently, partial stripes can only happen when closing the file at a
   *   non-stripe boundary, but this could also happen during (currently
   *   unimplemented) hflush/hsync support.
   * </p>
   */
  private long getAckedLength() {
    // Determine the number of full stripes that are sufficiently durable
    final long sentBytes = currentBlockGroup.getNumBytes();
    final long numFullStripes = sentBytes / numDataBlocks / cellSize;
    final long fullStripeLength = numFullStripes * numDataBlocks * cellSize;
    assert fullStripeLength <= sentBytes : "Full stripe length can't be " +
            "greater than the block group length";

    long ackedLength = 0;

    // Determine the length contained by at least `numDataBlocks` blocks.
    // Since it's sorted, all the blocks after `offset` are at least as long,
    // and there are at least `numDataBlocks` at or after `offset`.
    List<Long> blockLengths = Collections.unmodifiableList(getBlockLengths());
    List<Long> sortedBlockLengths = new ArrayList<>(blockLengths);
    Collections.sort(sortedBlockLengths);
    if (numFullStripes > 0) {
      final int offset = sortedBlockLengths.size() - numDataBlocks;
      ackedLength = sortedBlockLengths.get(offset) * numDataBlocks;
    }

    // If the acked length is less than the expected full stripe length, then
    // we're missing a full stripe. Return the acked length.
    if (ackedLength < fullStripeLength) {
      return ackedLength;
    }
    // If the expected length is exactly a stripe boundary, then we're also done
    if (ackedLength == sentBytes) {
      return ackedLength;
    }

    /*
    Otherwise, we're potentially dealing with a partial stripe.
    The partial stripe is laid out as follows:

      0 or more full data cells, `cellSize` in length.
      0 or 1 partial data cells.
      0 or more empty data cells.
      `numParityBlocks` parity cells, the length of the longest data cell.

    If the partial stripe is sufficiently acked, we'll update the ackedLength.
    */

    // How many full and empty data cells do we expect?
    final int numFullDataCells = (int)
            ((sentBytes - fullStripeLength) / cellSize);
    final int partialLength = (int) (sentBytes - fullStripeLength) % cellSize;
    final int numPartialDataCells = partialLength == 0 ? 0 : 1;
    final int numEmptyDataCells = numDataBlocks - numFullDataCells -
            numPartialDataCells;
    // Calculate the expected length of the parity blocks.
    final int parityLength = numFullDataCells > 0 ? cellSize : partialLength;

    final long fullStripeBlockOffset = fullStripeLength / numDataBlocks;

    // Iterate through each type of streamers, checking the expected length.
    long[] expectedBlockLengths = new long[numAllBlocks];
    int idx = 0;
    // Full cells
    for (; idx < numFullDataCells; idx++) {
      expectedBlockLengths[idx] = fullStripeBlockOffset + cellSize;
    }
    // Partial cell
    for (; idx < numFullDataCells + numPartialDataCells; idx++) {
      expectedBlockLengths[idx] = fullStripeBlockOffset + partialLength;
    }
    // Empty cells
    for (; idx < numFullDataCells + numPartialDataCells + numEmptyDataCells;
         idx++) {
      expectedBlockLengths[idx] = fullStripeBlockOffset;
    }
    // Parity cells
    for (; idx < numAllBlocks; idx++) {
      expectedBlockLengths[idx] = fullStripeBlockOffset + parityLength;
    }

    // Check expected lengths against actual streamer lengths.
    // Update if we have sufficient durability.
    int numBlocksWithCorrectLength = 0;
    for (int i = 0; i < numAllBlocks; i++) {
      if (blockLengths.get(i) == expectedBlockLengths[i]) {
        numBlocksWithCorrectLength++;
      }
    }
    if (numBlocksWithCorrectLength >= numDataBlocks) {
      ackedLength = sentBytes;
    }

    return ackedLength;
  }

  private int stripeDataSize() {
    return numDataBlocks * cellSize;
  }

  @Override
  public boolean hasCapability(String capability) {
    // StreamCapabilities like hsync / hflush are not supported yet.
    return false;
  }

  @Override
  public void hflush() {
    // not supported yet
    LOG.debug("DFSHybridOutputStream does not support hflush. "
            + "Caller should check StreamCapabilities before calling.");
  }

  @Override
  public void hsync() {
    // not supported yet
    LOG.debug("DFSHybridOutputStream does not support hsync. "
            + "Caller should check StreamCapabilities before calling.");
  }

  @Override
  public void hsync(EnumSet<SyncFlag> syncFlags) {
    // not supported yet
    LOG.debug("DFSHybridOutputStream does not support hsync {}. "
            + "Caller should check StreamCapabilities before calling.", syncFlags);
  }

  @Override
  protected synchronized void start() {
    // only starting the first streamer as it's the only one used
    streamers.get(0).start();
    for (StripedDataStreamer pstreamer : pstreamers) {
      pstreamer.start();
    }
  }

  @Override
  void abort() throws IOException {
    final MultipleIOException.Builder b = new MultipleIOException.Builder();
    synchronized (this) {
      if (isClosed()) {
        return;
      }
      exceptionLastSeen.set(new IOException("Lease timeout of "
              + (dfsClient.getConf().getHdfsTimeout() / 1000)
              + " seconds expired."));

      try {
        closeThreads(true);
      } catch (IOException e) {
        b.add(e);
      }
    }

    dfsClient.endFileLease(fileId);
    final IOException ioe = b.build();
    if (ioe != null) {
      throw ioe;
    }
  }

  @Override
  boolean isClosed() {
    if (closed) {
      return true;
    }
    for(HybridDataStreamer s : streamers) {
      if (!s.streamerClosed()) {
        return false;
      }
    }
    return true;
  }

  @Override
  protected void closeThreads(boolean force) throws IOException {
    final MultipleIOException.Builder b = new MultipleIOException.Builder();
    try {
      for (HybridDataStreamer streamer : streamers) {
        try {
          streamer.close(force);
          streamer.join();
          streamer.closeSocket();
        } catch (Exception e) {
          try {
            handleStreamerFailure("force=" + force, e, streamer);
          } catch (IOException ioe) {
            b.add(ioe);
          }
        } finally {
          streamer.setSocketToNull();
        }
      }
      for (StripedDataStreamer pstreamer : pstreamers) {
        try {
          pstreamer.close(force);
          pstreamer.join();
          pstreamer.closeSocket();
        } catch (Exception e) {

        } finally {
          pstreamer.setSocketToNull();
        }
      }
    } finally {
      setClosed();
    }
    final IOException ioe = b.build();
    if (ioe != null) {
      throw ioe;
    }
  }

  private boolean generateParityCellsForLastStripe() {
    final long currentBlockGroupBytes = currentBlockGroup == null ?
            0 : currentBlockGroup.getNumBytes();
    final long lastStripeSize = currentBlockGroupBytes % stripeDataSize();
    if (lastStripeSize == 0) {
      return false;
    }

    final long parityCellSize = lastStripeSize < cellSize?
            lastStripeSize : cellSize;
    final ByteBuffer[] buffers = cellBuffers.getBuffers();

    for (int i = 0; i < numDataBlocks + numParityBlocks; i++) {
      // Pad zero bytes to make all cells exactly the size of parityCellSize
      // If internal block is smaller than parity block, pad zero bytes.
      // Also pad zero bytes to all parity cells
      final int position = buffers[i].position();
      assert position <= parityCellSize : "If an internal block is smaller" +
              " than parity block, then its last cell should be small than last" +
              " parity cell";
      for (int j = 0; j < parityCellSize - position; j++) {
        buffers[i].put((byte) 0);
      }
      buffers[i].flip();
    }
    return true;
  }

  void writeParityCells() throws IOException {
    final ByteBuffer[] buffers = cellBuffers.getBuffers();
    // Skips encoding and writing parity cells if there are no healthy parity
    // data streamers
    if (!checkAnyParityStreamerIsHealthy()) {
      return;
    }
    //encode the data cells
    long start = System.currentTimeMillis();
    encode(encoder, numDataBlocks, buffers);

    long enc = System.currentTimeMillis();
    for (int i = 0; i < numParityBlocks; i++) {
      writeParity(i, buffers[numDataBlocks + i],
          cellBuffers.getChecksumArray(numDataBlocks + i));
    }

    long finish = System.currentTimeMillis();
    totalEncodingTime += (enc - start);
    totalParityWriteTime += (finish - enc);
  }

  private boolean checkAnyParityStreamerIsHealthy() {
    for (int i = 0; i < numParityBlocks; i++) {
      if (pstreamers.get(i).isHealthy()) {
        return true;
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Skips encoding and writing parity cells as there are "
              + "no healthy parity data streamers: " + pstreamers);
    }
    return false;
  }

  void writeParity(int index, ByteBuffer buffer, byte[] checksumBuf)
          throws IOException {
    final StripedDataStreamer current = setParityStreamer(index);
    final int len = buffer.limit();

    final long oldBytes = current.getBytesCurBlock();
    if (current.isHealthy()) {
      try {
        DataChecksum sum = getDataChecksum();
        if (buffer.isDirect()) {
          ByteBuffer directCheckSumBuf =
                  BUFFER_POOL.getBuffer(true, checksumBuf.length);
          sum.calculateChunkedSums(buffer, directCheckSumBuf);
          directCheckSumBuf.get(checksumBuf);
          BUFFER_POOL.putBuffer(directCheckSumBuf);
        } else {
          sum.calculateChunkedSums(buffer.array(), 0, len, checksumBuf, 0);
        }

        for (int i = 0; i < len; i += sum.getBytesPerChecksum()) {
          int chunkLen = Math.min(sum.getBytesPerChecksum(), len - i);
          int ckOffset = i / sum.getBytesPerChecksum() * getChecksumSize();
          super.writeParityChunk(buffer, chunkLen, checksumBuf, ckOffset,
                  getChecksumSize());
        }
      } catch(Exception e) {
        handleCurrentStreamerFailure("oldBytes=" + oldBytes + ", len=" + len,
                e);
      }
    }
  }

  @Override
  void setClosed() {
    super.setClosed();
    getHybridDataStreamer(0).release();
    for (int i = 0; i < numParityBlocks; i++) {
      getHybridParityStreamer(i).release();
    }
    cellBuffers.release();
  }

  @Override
  protected synchronized void closeImpl() throws IOException {
    long start = System.currentTimeMillis();
    if (isClosed()) {
      LOG.info("Closing an already closed stream. [Stream:{}, streamer:{}]",
              closed, getStreamer().streamerClosed());
      try {
        getStreamer().getLastException().check(true);
      } finally {
        if (!closed) {
          // If stream is not closed but streamer closed, clean up the stream.
          // Most importantly, end the file lease.
          closeThreads(true);
        }
      }
      return;
    }

    try {
      final long startTime = System.currentTimeMillis();
      flushBuffer();       // flush from all upper layers

      // generate and send last stripe parities
      if (paritiesEnabled) {
        if (generateParityCellsForLastStripe()) {
          writeParityCells();
        }

        if (pstreamer.getBytesCurBlock() != 0) {
          for (int p = 0; p < numParityBlocks; p++) {
            setParityStreamer(p);
            setParityPacketToEmpty();
            enqueueParityPacket();
            flushParity();
          }
        }
      }

      if (currentPacket != null) {
        enqueueCurrentPacket();
      }

      // block group is not fully complete, need to end stripes
      if (currentBlockGroup.getNumBytes() != blockSize * numDataBlocks) {
        setCurrentPacketToEmpty();
      }

      try {
        flushData();             // flush all data to Datanodes
      } catch (IOException ioe) {
        LOG.warn("Unable to flush data when completing", ioe);
      }

      final long completeTime = System.currentTimeMillis();
      try (TraceScope ignored =
               dfsClient.getTracer().newScope("completeFile")) {
        if (getCurrentStreamer().getBlock() == null) {
          // sometimes this happens when things fail, figure out a better resolution for this
          LOG.warn("Current replica set is null, passing in nothing to complete file");
          completeFile(currentBlockGroup, new ExtendedBlock[]{});
        } else {
          // set and send for commit
          ExtendedBlock currParity = new ExtendedBlock(currentParityGroup);
          currParity.setNumBytes((numParityBlocks + secretNumParities) * StripedBlockUtil.getInternalBlockLength(
              currentBlockGroup.getNumBytes(), cellSize, numDataBlocks, 0));

          ExtendedBlock currReplica = new ExtendedBlock(currentReplicaSet);
          currReplica.setNumBytes(getCurrentStreamer().getBlock().getNumBytes());

          completeFile(currentBlockGroup, new ExtendedBlock[]{currParity, currReplica});
        }
      }
      LOG.info("Took {} ms to flush and {} ms to complete",
          (completeTime - startTime), (System.currentTimeMillis() - completeTime));
    } catch (ClosedChannelException ignored) {
    } finally {
      // Failures may happen when flushing data.
      // Streamers may keep waiting for the new block information.
      // Thus need to force closing these threads.
      // Don't need to call setClosed() because closeThreads(true)
      // calls setClosed() in the finally block.
      closeThreads(true);
      totalCloseTime += System.currentTimeMillis() - start;
      if (false) {
        logMetrics();
      }
    }
  }

  private void logMetrics() {
    LOG.info("Total time to initialize = " + totalInitTime + " ms.");
    LOG.info("Total time to allocate group = " + totalGroupAllocTime + " ms.");
    LOG.info("Total time to allocate replicas = " + totalReplicaAllocTime + " ms.");
    LOG.info("Total time to wait = " + totalWaitingTime + " ms.");
    LOG.info("Total time to send data = " + (totalDataWriteTime / (1000 * 1000)) + " ms.");
    LOG.info("Total time to send parity = " + totalParityWriteTime + " ms.");
    LOG.info("Total time to encode = " + totalEncodingTime + " ms.");
    LOG.info("Total time to close = " + totalCloseTime + " ms.");
    LOG.info("Total time end-to-end = " + (System.currentTimeMillis() - globalStartTime) + " ms.");
  }

  @VisibleForTesting
  void enqueueAllCurrentPackets() throws IOException {
    int idx = streamers.indexOf(getCurrentStreamer());
    for(int i = 0; i < streamers.size(); i++) {
      final HybridDataStreamer si = setCurrentStreamer(i);
      if (si.isHealthy() && currentPacket != null) {
        try {
          enqueueCurrentPacket();
        } catch (IOException e) {
          handleCurrentStreamerFailure("enqueueAllCurrentPackets, i=" + i, e);
        }
      }
    }
    setCurrentStreamer(idx);
  }

  void flushDataInternals() throws IOException {
    Map<Future<Void>, Integer> flushAllFuturesMap = new HashMap<>();
    Future<Void> future = null;
    int current = getCurrentIndex();

    for (int i = 0; i < numDataBlocks; i++) {
      final HybridDataStreamer s = setCurrentStreamer(i);
      if (s.isHealthy()) {
        try {
          // flush all data to Datanode
          final long toWaitFor = flushInternalWithoutWaitingAck();
          future = flushAllExecutorCompletionService.submit(
                  new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                      s.waitForAckedSeqno(toWaitFor);
                      return null;
                    }
                  });
          flushAllFuturesMap.put(future, i);
        } catch (Exception e) {
          handleCurrentStreamerFailure("flushInternal " + s, e);
        }
      }
    }
    setCurrentStreamer(current);
    for (int i = 0; i < flushAllFuturesMap.size(); i++) {
      try {
        future = flushAllExecutorCompletionService.take();
        future.get();
      } catch (InterruptedException ie) {
        throw DFSUtilClient.toInterruptedIOException(
                "Interrupted during waiting all streamer flush, ", ie);
      } catch (ExecutionException ee) {
        LOG.warn(
                "Caught ExecutionException while waiting all streamer flush, ", ee);
        HybridDataStreamer s = streamers.get(flushAllFuturesMap.get(future));
        handleStreamerFailure("flushInternal " + s,
                (Exception) ee.getCause(), s);
      }
    }
  }

  void flushAllInternals() throws IOException {
    flushDataInternals();
    flushParityInternals();
  }

  void flushParityInternals() throws IOException {
    Map<Future<Void>, Integer> flushAllFuturesMap = new HashMap<>();
    Future<Void> future = null;
    int current = getParityIndex() - numDataBlocks; // subtract to get physical position in list

    // TODO: validate correct indices
    for (int i = numDataBlocks; i < numAllBlocks; i++) {
      final StripedDataStreamer s = setParityStreamer(i);
      if (s.isHealthy()) {
        try {
          // flush all data to Datanode
          final long toWaitFor = flushInternalParityWithoutWaitingAck();
          future = flushAllExecutorCompletionService.submit(
                  () -> {
                    s.waitForAckedSeqno(toWaitFor);
                    return null;
                  });
          flushAllFuturesMap.put(future, i);
        } catch (Exception e) {
          handleCurrentStreamerFailure("flushInternal " + s, e);
        }
      }
    }
    setParityStreamer(current);
    for (int i = 0; i < flushAllFuturesMap.size(); i++) {
      try {
        future = flushAllExecutorCompletionService.take();
        future.get();
      } catch (InterruptedException ie) {
        throw DFSUtilClient.toInterruptedIOException(
                "Interrupted during waiting all streamer flush, ", ie);
      } catch (ExecutionException ee) {
        LOG.warn(
                "Caught ExecutionException while waiting all streamer flush, ", ee);
        StripedDataStreamer s = pstreamers.get(flushAllFuturesMap.get(future));
        s.getErrorState().setInternalError();
        s.close(true);
        checkStreamers();
        currentPackets[s.getIndex()] = null;
      }
    }
  }

  static void sleep(long ms, String op) throws InterruptedIOException {
    try {
      Thread.sleep(ms);
    } catch(InterruptedException ie) {
      throw DFSUtilClient.toInterruptedIOException(
              "Sleep interrupted during " + op, ie);
    }
  }

  private void logCorruptBlocks() {
    for (Map.Entry<Integer, Integer> entry : corruptBlockCountMap.entrySet()) {
      int bgIndex = entry.getKey();
      int corruptBlockCount = entry.getValue();
      StringBuilder sb = new StringBuilder();
      sb.append("Block group <").append(bgIndex).append("> failed to write ")
              .append(corruptBlockCount).append(" blocks.");
      if (corruptBlockCount == numAllBlocks - numDataBlocks) {
        sb.append(" It's at high risk of losing data.");
      }
      LOG.warn(sb.toString());
    }
  }

  @Override
  ExtendedBlock getBlock() {
    return currentBlockGroup;
  }
}