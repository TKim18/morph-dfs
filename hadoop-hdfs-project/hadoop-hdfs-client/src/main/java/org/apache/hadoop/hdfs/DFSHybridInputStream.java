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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSUtilClient.CorruptedBlocks;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;


/**
 * DFSHybridInputStream reads from hybrid block groups.
 * Much of the implementation between this class and @DFSStripedInputStream
 * are the same. In fact, this class is a combination of @DFSInputStream and
 * the striped input stream implementation.
 *
 * Reads are implemented as follows. For each read, it can be serviced via
 * direct replica read as in DFSInputStream or a striped replica read via
 * DFSStripedInputStream. Based on a configuration/property, reads are defaulted
 * to be read from one or both.
 *
 * The idea is that each individual type of read is implemented in effectively
 * the same way as the others.
 */
@InterfaceAudience.Private
public class DFSHybridInputStream extends DFSStripedInputStream {

  final boolean readReplica = true;
  final boolean readStripe = true;
  final boolean metricsEnabled = true;
  boolean paritiesEnabled = true;

  final int hedgedReadPoolSize;
  final long hedgedReadThreshold;
  final boolean injectDelay = false;
  final long injectDelayTimeMillis = 200;

  LocatedStripedBlock parity = null;

  DFSHybridInputStream(DFSClient dfsClient, String src,
                       boolean verifyChecksum, ErasureCodingPolicy ecPolicy,
                       LocatedBlocks locatedBlocks) throws IOException {
    super(dfsClient, src, verifyChecksum, ecPolicy, locatedBlocks);

    hedgedReadPoolSize = dfsClient.getConf().getHedgedReadThreadpoolSize();
    hedgedReadThreshold = dfsClient.getConf().getHedgedReadThresholdMillis();

    int hack = dfsClient.getConf().getMaxRetryAttempts();
    if (hack < 10 && hack != 8) {
      paritiesEnabled = false;
      parityBlkNum = 0;
    }
  }

  /**
   * Real implementation of pread.
   */
  @Override
  protected void fetchBlockByteRange(LocatedBlock block, long start,
                                     long end, ByteBuffer buf, CorruptedBlocks corruptedBlocks)
      throws IOException {
    // the idea here is to consider both replicas and striped blocks
    final long startTime = System.currentTimeMillis();
    try {
      assert block.isHybrid();
      LocatedHybridBlock hblk = (LocatedHybridBlock) block;

      // replica reads don't have metadata in the right form
      // construct a new located block based on what's inside the hybrid block
      // construct a new striped block based on what's inside the hybrid block
      LocatedBlock[] replicas = parseReplicaSets(hblk);
      LocatedStripedBlock stripe = parseStripeBlock(hblk);

      // read to the entire block group, do a striped read
      final boolean fullBlockRead = end - start + 1 == block.getBlockSize();
      if (fullBlockRead) {
        readStripeBlock(stripe, start, end, buf, corruptedBlocks, new ArrayList<>());
      } else {
        hedgedReadRange(replicas, stripe, start, end, buf, corruptedBlocks);
      }

      /*
      if (readReplica && readStripe) {
        // both are enabled, use a hedged bet approach to read both
        hedgedReadRange(replicas, stripe, start, end, buf, corruptedBlocks);
      } else if (readReplica) {
        // only read contiguous is enabled, potentially use a hedged
        // bet approach to reading from the various replicas
        // though not always enabled, could be single replica read
        readReplicas(replicas, start, end, buf, corruptedBlocks);
      } else if (readStripe) {
        // only read striped, probably really only useful for benchmarking
        // and shouldn't be enabled for real performance reasons
        readStripeBlock(stripe, start, end, buf, corruptedBlocks);
      }
      */

    } catch (IOException e) {
      DFSClient.LOG.error("Encountered an error while reading hybrid file: ", e);
    } finally {
      final long endTime = System.currentTimeMillis();
      DFSClient.LOG.debug("Time to read when replica_enabled={} " +
              "and hedged_replica_enabled={} " +
              "and stripe_enabled={} took {} ms",
          readReplica, dfsClient.isHedgedReadsEnabled(),
          readStripe, endTime - startTime);
    }
  }

  private LocatedBlock[] parseReplicaSets(LocatedHybridBlock hblk) {
    // there will be multiple located blocks
    ExtendedBlock[] replicas = hblk.getReplicas();
    int numReplicaSets = replicas.length;
    int numStripeUnits = dataBlkNum + parityBlkNum;
    int numAllMachines = hblk.getLocations().length;
    int replication = (numAllMachines - numStripeUnits) / numReplicaSets;

    LocatedBlock[] rblks = new LocatedBlock[numReplicaSets];
    long offset = hblk.getStartOffset();
    int i, j, pos;
    for (i = numStripeUnits; i < numAllMachines; i += replication) {
      pos = (i - numStripeUnits) / replication;

      DatanodeInfo[] locations = new DatanodeInfo[replication];
      String[] storageIds = new String[replication];
      StorageType[] storageTypes = new StorageType[replication];

      for (j = 0; j < replication; j++) {
        locations[j] = hblk.getLocations()[i + j];
        storageIds[j] = hblk.getStorageIDs()[i + j];
        storageTypes[j] = hblk.getStorageTypes()[i + j];
      }

      rblks[pos] = new LocatedBlock(
          replicas[pos], locations, storageIds, storageTypes,
          offset, hblk.isCorrupt(), hblk.getCachedLocations());

      offset += replicas[pos].getNumBytes();
    }

    return rblks;
  }

  private LocatedStripedBlock parseStripeBlock(LocatedHybridBlock hblk) {
    // build striped block
    DatanodeInfo[] locations = new DatanodeInfo[dataBlkNum];
    String[] storageIds = new String[dataBlkNum];
    StorageType[] storageTypes = new StorageType[dataBlkNum];

    // populate striped node data
    int i = 0;
    for (; i < dataBlkNum; i++) {
      locations[i] = hblk.getLocations()[i];
      storageIds[i] = hblk.getStorageIDs()[i];
      storageTypes[i] = hblk.getStorageTypes()[i];
    }

    // save parity block metadatas
    ExtendedBlock parityBlock = hblk.getParity();
    if (parityBlock != null) {
      DatanodeInfo[] plocations = new DatanodeInfo[parityBlkNum];
      String[] pstorageIds = new String[parityBlkNum];
      StorageType[] pstorageTypes = new StorageType[parityBlkNum];

      // populate striped node data
      for (i = dataBlkNum; i < dataBlkNum + parityBlkNum; i++) {
        plocations[i - dataBlkNum] = hblk.getLocations()[i];
        pstorageIds[i - dataBlkNum] = hblk.getStorageIDs()[i];
        pstorageTypes[i - dataBlkNum] = hblk.getStorageTypes()[i];
      }

      byte[] pindices = new byte[parityBlkNum];
      for (byte idx = 0; idx < parityBlkNum; idx++) {
        pindices[idx] = idx;
      }

      parity = new LocatedStripedBlock(
          parityBlock, plocations, pstorageIds, pstorageTypes,
          pindices, hblk.getStartOffset(),
          hblk.isCorrupt(), hblk.getCachedLocations());
    }

    // repopulate block indices
    int numStripeUnits = dataBlkNum + parityBlkNum;
    byte[] blockIndices = new byte[numStripeUnits];
    for (byte idx = 0; idx < numStripeUnits; idx++) {
      blockIndices[idx] = idx;
    }

    return new LocatedStripedBlock(
        hblk.getBlock(), locations, storageIds, storageTypes,
        blockIndices, hblk.getStartOffset(),
        hblk.isCorrupt(), hblk.getCachedLocations());
  }

  private void hedgedReadRange(LocatedBlock[] replicas, LocatedStripedBlock stripe, long start, long end,
                          ByteBuffer buf, CorruptedBlocks corruptedBlocks) throws IOException {
    // need to iterate each replica to be read on the replicated range
    // for each range, you need to do a hedged read
    long remaining = (end + 1) - start;
    long targetStart, targetEnd, bytesToRead;
    long position = start;

    for (int i = 0; i < replicas.length && remaining > 0; i++) {
      LocatedBlock block = replicas[i];

      targetStart = position - block.getStartOffset();
      bytesToRead = Math.min(remaining, block.getBlockSize() - targetStart);
      targetEnd = targetStart + bytesToRead - 1;

      if (targetEnd - targetStart > 0) {
        doHedgedRead(block, stripe, targetStart, targetEnd, buf, corruptedBlocks);
        remaining -= bytesToRead;
        position += bytesToRead;
      }
    }
  }

  private void doHedgedRead(LocatedBlock replica, LocatedStripedBlock stripe,
      long start, long end, ByteBuffer buf, CorruptedBlocks corruptedBlocks) throws IOException {
    // similar to impl for replicated hedged read
    final long startTime = System.currentTimeMillis();
    ArrayList<Future<ByteBuffer>> futures = new ArrayList<>();
    CompletionService<ByteBuffer> hedgedService =
        new ExecutorCompletionService<>(dfsClient.getHedgedReadsThreadPool());
    ArrayList<DatanodeInfo> ignored = new ArrayList<>();
    ByteBuffer bb;
    boolean firstTry = true;
    boolean triedStripe = false;
    Future<ByteBuffer> firstRequest = null, oneMoreRequest = null, stripeRequest = null;
    int len = (int) (end - start + 1);
    while (true) {
      DNAddrPair chosenNode = null;
      // this is the first request (replica)
      if (firstTry) {
        firstTry = false;
        chosenNode = chooseDataNode(replica, ignored);
        replica = chosenNode.block;
        bb = ByteBuffer.allocate(len);
        firstRequest = hedgedService.submit(
            getFromReplica(chosenNode, start, end, bb, corruptedBlocks));
        futures.add(firstRequest);
//        DFSClient.LOG.info("Spawned first read to replica {} after {} ms",
//            chosenNode.info, System.currentTimeMillis() - startTime);
        Future<ByteBuffer> future = null;
        try {
          future = hedgedService.poll(hedgedReadThreshold, TimeUnit.MILLISECONDS);
          if (future != null) {
            ByteBuffer result = future.get();
            result.flip();
            buf.put(result);
            DFSClient.LOG.info("Successfully read from first replica in {} ms",
                System.currentTimeMillis() - startTime);
            return;
          }
        } catch (ExecutionException e) {
          futures.remove(future);
        } catch (InterruptedException e) {
          throw new InterruptedIOException(
              "Interrupted while waiting for reading task");
        }
        // Ignore this node on next go around.
        // If poll timeout and the request still ongoing, don't consider it
        // again. If read data failed, don't consider it either.
        ignored.add(chosenNode.info);
      } else {
        // We are starting up a 'hedged' read. We have a read already
        // ongoing.
        try {
          chosenNode = chooseDataNode(replica, ignored, false);
          bb = ByteBuffer.allocate(len);

          if (chosenNode != null) {
            // Latest block, if refreshed internally
            replica = chosenNode.block;
            oneMoreRequest = hedgedService.submit(
                getFromReplica(chosenNode, start, end, bb, corruptedBlocks));
            futures.add(oneMoreRequest);
            DFSClient.LOG.info("Spawning second read to replica {} after {} ms",
                chosenNode.info, System.currentTimeMillis() - startTime);
          } else {
            if (!triedStripe) {
              // need to do striped read here as we're out of replicas
              stripeRequest = hedgedService.submit(
                  getFromStripe(stripe, start, end, bb, corruptedBlocks, ignored));
              futures.add(stripeRequest);
              triedStripe = true;
              DFSClient.LOG.info("Spawning striped read {} after {} ms ",
                  stripe.getBlock().getBlockId(), System.currentTimeMillis() - startTime);
            } else {
              // already tried stripe, only option is to just wait now
            }
          }
        } catch (IOException ioe) {
          DFSClient.LOG.warn("Failed getting node for hedged read: {}", ioe.getMessage());
        }
        // keep trying to poll, after threshold, try a new source
        Future<ByteBuffer> future = null;
        try {
          future = hedgedService.poll(hedgedReadThreshold, TimeUnit.MILLISECONDS);
          if (future != null) {
            ByteBuffer result = future.get();
            cancelAll(futures);
            result.flip();
            buf.put(result);
            if (future == firstRequest) {
              DFSClient.LOG.info("Successfully read from first replica in {} ms",
                  System.currentTimeMillis() - startTime);
            } else if (future == oneMoreRequest) {
              DFSClient.LOG.info("Successfully read from second replica in {} ms",
                  System.currentTimeMillis() - startTime);
            } else if (future == stripeRequest){
              DFSClient.LOG.info("Successfully read from stripe in {} ms",
                  System.currentTimeMillis() - startTime);
            } else {
              DFSClient.LOG.info("Not entirely sure what we caught");
            }

            return;
          }
        } catch (ExecutionException e) {
          futures.remove(future);
        } catch (InterruptedException e) {
          // Ignore and retry
        }
        // We got here if exception. Ignore this node on next go around IFF
        // we found a chosenNode to hedge read against.
        if (chosenNode != null && chosenNode.info != null) {
          ignored.add(chosenNode.info);
        }
      }
    }
  }

  private void readReplicas(
      final LocatedBlock[] replicas, final long start, final long end,
      final ByteBuffer bb, final CorruptedBlocks corruptedBlocks) throws IOException {
    // we need to determine where to begin and end reading a block
    long remaining = (end + 1) - start;
    long targetStart, targetEnd, bytesToRead;
    long position = start;

    for (int i = 0; i < replicas.length && remaining > 0; i++) {
      LocatedBlock block = replicas[i];

      targetStart = position - block.getStartOffset();
      bytesToRead = Math.min(remaining, block.getBlockSize() - targetStart);
      targetEnd = targetStart + bytesToRead - 1;

      DNAddrPair addressPair = chooseDataNode(block, null);
      try {
        actualGetFromOneDataNode(addressPair, targetStart, targetEnd, bb, corruptedBlocks);
      } catch (IOException e) {
        checkInterrupted(e); // check if the read has been interrupted
      }

      remaining -= bytesToRead;
      position += bytesToRead;
    }
  }

  private Callable<ByteBuffer> getFromReplica(
      final DNAddrPair datanode, final long start, final long end,
      final ByteBuffer bb, final CorruptedBlocks corruptedBlocks) {
    return () -> {
      if (injectDelay) {
        Thread.sleep(injectDelayTimeMillis);
      }
      try {
        actualGetFromOneDataNode(datanode, start, end, bb, corruptedBlocks);
        return bb;
      } catch (InterruptedIOException e) {
        return null;
      } catch (IOException e) {
        DFSClient.LOG.error("Getting from datanode threw an exception", e);
        throw e;
      }
    };
  }

  /**
   * Stripe read counterpart to readReplicas.
   * @throws IOException
   */
  private void readStripeBlock(LocatedStripedBlock block, long start, long end,
                               ByteBuffer buf, CorruptedBlocks corruptedBlocks,
                               List<DatanodeInfo> triedNodes) throws IOException {
    final long startTime = System.currentTimeMillis();
    StripedBlockUtil.AlignedStripe[] stripes = StripedBlockUtil.divideByteRangeIntoStripes(
        ecPolicy, cellSize, block, start, end, buf);
    final LocatedBlock[] blks = StripedBlockUtil.parseStripedBlockGroupWithParities(
        block, parity, cellSize, dataBlkNum, parityBlkNum);
    boolean degradedRead = false;
    int missingUnits = 0;
    // want to parse out the parities for this group too
    final StripeReader.BlockReaderInfo[] preaderInfos = new StripeReader.BlockReaderInfo[totalBlkNum];
    long readTo = -1;
    for (StripedBlockUtil.AlignedStripe stripe : stripes) {
      readTo = Math.max(readTo, stripe.getOffsetInBlock() + stripe.getSpanInBlock());
    }
    try {
      for (StripedBlockUtil.AlignedStripe stripe : stripes) {
        // Parse group to get chosen DN location
        StripeReader preader = new PositionStripeReader(stripe, ecPolicy, blks,
            preaderInfos, corruptedBlocks, decoder, this);
        preader.readTo = readTo;
        try {
          preader.readStripe();
        } finally {
          degradedRead |= preader.degradedRead;
          missingUnits = Math.max(missingUnits, preader.missingUnits);
          triedNodes.addAll(preader.failedNodes);
          preader.close();
        }
      }
      buf.position(buf.position() + (int)(end - start + 1));
    } finally {
      for (StripeReader.BlockReaderInfo preaderInfo : preaderInfos) {
        closeReader(preaderInfo);
      }
      DFSClient.LOG.info("Completed hybrid striped read for {} | full degraded mode = {}, missing = {} units, striped read in {} ms",
          block.getBlock().getBlockId(), degradedRead, missingUnits, System.currentTimeMillis() - startTime);
      DFSClient.LOG.info("All failed nodes for {}: {}", block.getBlock().getBlockId(), Arrays.toString(triedNodes.toArray()));
    }
  }

  private Callable<ByteBuffer> getFromStripe(
      final LocatedStripedBlock stripe, final long start, final long end,
      final ByteBuffer bb, final CorruptedBlocks corruptedBlocks, final ArrayList<DatanodeInfo> triedNodes) {
    return () -> {
      readStripeBlock(stripe, start, end, bb, corruptedBlocks, triedNodes);
      return bb;
    };
  }

  private void cancelAll(List<Future<ByteBuffer>> futures) {
    for (Future<ByteBuffer> future : futures) {
      future.cancel(false);
    }
  }

  @Override
  protected void reportLostBlock(LocatedBlock lostBlock,
                                 Collection<DatanodeInfo> ignoredNodes) {
    // quench warning for now, possibly add back in the future
  }

}
