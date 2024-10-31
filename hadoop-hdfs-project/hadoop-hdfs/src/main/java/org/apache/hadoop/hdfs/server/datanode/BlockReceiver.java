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
package org.apache.hadoop.hdfs.server.datanode;

import static org.apache.hadoop.hdfs.server.datanode.DataNode.DN_CLIENTTRACE_FORMAT;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.Checksum;

import org.apache.commons.logging.Log;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSOutputSummer;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketReceiver;
import org.apache.hadoop.hdfs.protocol.datatransfer.PipelineAck;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaInputStreams;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodePeerMetrics;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.htrace.core.Span;
import org.apache.htrace.core.Tracer;

import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.POSIX_FADV_DONTNEED;
import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.SYNC_FILE_RANGE_WRITE;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;

/** A class that receives a block and writes to its own disk, meanwhile
 * may copies it to another site. If a throttler is provided,
 * streaming throttling is also supported.
 **/
class BlockReceiver implements Closeable {
  public static final Logger LOG = DataNode.LOG;
  static final Log ClientTraceLog = DataNode.ClientTraceLog;

  @VisibleForTesting
  static long CACHE_DROP_LAG_BYTES = 8 * 1024 * 1024;
  protected final long datanodeSlowLogThresholdMs;
  protected DataInputStream in = null; // from where data are read
  protected DataChecksum clientChecksum; // checksum used by client
  protected DataChecksum diskChecksum; // checksum we write to disk

  /**
   * In the case that the client is writing with a different
   * checksum polynomial than the block is stored with on disk,
   * the DataNode needs to recalculate checksums before writing.
   */
  protected final boolean needsChecksumTranslation;
  protected DataOutputStream checksumOut = null; // to crc file at local disk
  protected final int bytesPerChecksum;
  protected final int checksumSize;

  protected final PacketReceiver packetReceiver = new PacketReceiver(false);

  protected final String inAddr;
  protected final String myAddr;
  protected String mirrorAddr;
  protected String mirrorNameForMetrics;
  protected DataOutputStream mirrorOut;

  protected MirrorInfo[] mirrors = null;
  protected Daemon responder = null;
  protected int stripeIndex;
  protected PacketResponderRole role;
  protected boolean stripedWrite = false;
  // Map used for striped writes to track when to move inputStreams
  protected ConcurrentLinkedDeque<Pair<Long, DataInputStream>> lastSeqno;
  // TODO: pass in cellSize based on config
  protected int cellSize;
  protected DataTransferThrottler throttler;
  protected ReplicaOutputStreams streams;
  protected DatanodeInfo srcDataNode = null;
  protected DatanodeInfo[] downstreamDNs = DatanodeInfo.EMPTY_ARRAY;
  protected final DataNode datanode;
  volatile protected boolean mirrorError;

  // Cache management state
  protected boolean dropCacheBehindWrites;
  protected long lastCacheManagementOffset = 0;
  protected boolean syncBehindWrites;
  protected boolean syncBehindWritesInBackground;

  /** The client name.  It is empty if a datanode is the client */
  protected final String clientname;
  protected final boolean isClient;
  protected final boolean isDatanode;

  /** the block to receive */
  protected final ExtendedBlock block;
  /** the replica to write */
  protected ReplicaInPipeline replicaInfo;
  /** pipeline stage */
  protected final BlockConstructionStage stage;
  protected final boolean isTransfer;
  protected boolean isPenultimateNode = false;

  protected boolean syncOnClose;
  protected volatile boolean dirSyncOnFinalize;
  protected boolean dirSyncOnHSyncDone = false;
  protected long restartBudget;
  /** the reference of the volume where the block receiver writes to */
  protected ReplicaHandler replicaHandler;
  protected ParityHandler parityHandler = null;

  /**
   * for replaceBlock response
   */
  protected final long responseInterval;
  protected long lastResponseTime = 0;
  protected boolean isReplaceBlock = false;
  protected DataOutputStream replyOut = null;
  protected long maxWriteToDiskMs = 0;

  protected boolean pinning;
  protected final AtomicLong lastSentTime = new AtomicLong(0L);
  protected long maxSendIdleTime;

  BlockReceiver(final ExtendedBlock block, final StorageType storageType,
      final DataInputStream in,
      final String inAddr, final String myAddr,
      final BlockConstructionStage stage,
      final long newGs, final long minBytesRcvd, final long maxBytesRcvd,
      final String clientname, final DatanodeInfo srcDataNode,
      final DataNode datanode, DataChecksum requestedChecksum,
      CachingStrategy cachingStrategy,
      final boolean allowLazyPersist,
      final boolean allowReplicaCache,
      final boolean pinning,
      final String storageId) throws IOException {
    try{
      this.block = block;
      this.in = in;
      this.inAddr = inAddr;
      this.myAddr = myAddr;
      this.srcDataNode = srcDataNode;
      this.datanode = datanode;

      this.clientname = clientname;
      this.isDatanode = clientname.length() == 0;
      this.isClient = !this.isDatanode;
      this.restartBudget = datanode.getDnConf().restartReplicaExpiry;
      this.datanodeSlowLogThresholdMs =
          datanode.getDnConf().getSlowIoWarningThresholdMs();
      // For replaceBlock() calls response should be sent to avoid socketTimeout
      // at clients. So sending with the interval of 0.5 * socketTimeout
      final long readTimeout = datanode.getDnConf().socketTimeout;
      this.responseInterval = (long) (readTimeout * 0.5);
      //for datanode, we have
      //1: clientName.length() == 0, and
      //2: stage == null or PIPELINE_SETUP_CREATE
      this.stage = stage;
      this.isTransfer = stage == BlockConstructionStage.TRANSFER_RBW
          || stage == BlockConstructionStage.TRANSFER_FINALIZED;

      this.pinning = pinning;
      this.lastSentTime.set(Time.monotonicNow());
      // Downstream will timeout in readTimeout on receiving the next packet.
      // If there is no data traffic, a heartbeat packet is sent at
      // the interval of 0.5*readTimeout. Here, we set 0.9*readTimeout to be
      // the threshold for detecting congestion.
      this.maxSendIdleTime = (long) (readTimeout * 0.9);
      if (LOG.isDebugEnabled()) {
        LOG.debug(getClass().getSimpleName() + ": " + block
            + "\n storageType=" + storageType + ", inAddr=" + inAddr
            + ", myAddr=" + myAddr + "\n stage=" + stage + ", newGs=" + newGs
            + ", minBytesRcvd=" + minBytesRcvd
            + ", maxBytesRcvd=" + maxBytesRcvd + "\n clientname=" + clientname
            + ", srcDataNode=" + srcDataNode
            + ", datanode=" + datanode.getDisplayName()
            + "\n requestedChecksum=" + requestedChecksum
            + "\n cachingStrategy=" + cachingStrategy
            + "\n allowLazyPersist=" + allowLazyPersist + ", pinning=" + pinning
            + ", isClient=" + isClient + ", isDatanode=" + isDatanode
            + ", responseInterval=" + responseInterval
            + ", storageID=" + (storageId != null ? storageId : "null")
        );
      }

      //
      // Open local disk out
      //
      if (isDatanode) { //replication or move
        replicaHandler =
            datanode.data.createTemporary(storageType, storageId, block, false);
      } else {
        switch (stage) {
        case PIPELINE_SETUP_CREATE:
          replicaHandler = datanode.data.createRbw(storageType, storageId,
              block, allowLazyPersist, allowReplicaCache);
          datanode.notifyNamenodeReceivingBlock(
              block, replicaHandler.getReplica().getStorageUuid());
          break;
        case PIPELINE_SETUP_STREAMING_RECOVERY:
          replicaHandler = datanode.data.recoverRbw(
              block, newGs, minBytesRcvd, maxBytesRcvd);
          block.setGenerationStamp(newGs);
          break;
        case PIPELINE_SETUP_APPEND:
          replicaHandler = datanode.data.append(block, newGs, minBytesRcvd);
          block.setGenerationStamp(newGs);
          datanode.notifyNamenodeReceivingBlock(
              block, replicaHandler.getReplica().getStorageUuid());
          break;
        case PIPELINE_SETUP_APPEND_RECOVERY:
          replicaHandler = datanode.data.recoverAppend(block, newGs, minBytesRcvd);
          block.setGenerationStamp(newGs);
          datanode.notifyNamenodeReceivingBlock(
              block, replicaHandler.getReplica().getStorageUuid());
          break;
        case TRANSFER_RBW:
        case TRANSFER_FINALIZED:
          // this is a transfer destination
          replicaHandler = datanode.data.createTemporary(storageType, storageId,
              block, isTransfer);
          break;
        default: throw new IOException("Unsupported stage " + stage +
              " while receiving block " + block + " from " + inAddr);
        }
      }
      replicaInfo = replicaHandler.getReplica();
//      LOG.info("BlockReceiver: ReplicaInfo = {}", replicaInfo.getReplicaInfo().toString());
      this.dropCacheBehindWrites = (cachingStrategy.getDropBehind() == null) ?
        datanode.getDnConf().dropCacheBehindWrites :
          cachingStrategy.getDropBehind();
      this.syncBehindWrites = datanode.getDnConf().syncBehindWrites;
      this.syncBehindWritesInBackground = datanode.getDnConf().
          syncBehindWritesInBackground;

      final boolean isCreate = isDatanode || isTransfer
          || stage == BlockConstructionStage.PIPELINE_SETUP_CREATE;
      streams = replicaInfo.createStreams(isCreate, requestedChecksum);
      assert streams != null : "null streams!";

      // read checksum meta information
      this.clientChecksum = requestedChecksum;
      this.diskChecksum = streams.getChecksum();
      this.needsChecksumTranslation = !clientChecksum.equals(diskChecksum);
      this.bytesPerChecksum = diskChecksum.getBytesPerChecksum();
      this.checksumSize = diskChecksum.getChecksumSize();

      this.checksumOut = new DataOutputStream(new BufferedOutputStream(
          streams.getChecksumOut(), DFSUtilClient.getSmallBufferSize(
          datanode.getConf())));
      // write data chunk header if creating a new replica
      if (isCreate) {
        BlockMetadataHeader.writeHeader(checksumOut, diskChecksum);
      }
    } catch (ReplicaAlreadyExistsException bae) {
      throw bae;
    } catch (ReplicaNotFoundException bne) {
      throw bne;
    } catch(IOException ioe) {
      if (replicaInfo != null) {
        replicaInfo.releaseAllBytesReserved();
      }
      IOUtils.closeStream(this);
      cleanupBlock();

      // check if there is a disk error
      IOException cause = DatanodeUtil.getCauseIfDiskError(ioe);
      DataNode.LOG
          .warn("IOException in BlockReceiver constructor :" + ioe.getMessage()
          + (cause == null ? "" : ". Cause is "), cause);
      if (cause != null) {
        ioe = cause;
        // Volume error check moved to FileIoProvider
      }

      throw ioe;
    }
  }

  /** Return the datanode object. */
  DataNode getDataNode() {return datanode;}

  Replica getReplica() {
    return replicaInfo;
  }

  /**
   * close files and release volume reference.
   */
  @Override
  public void close() throws IOException {
    Span span = Tracer.getCurrentSpan();
    if (span != null) {
      span.addKVAnnotation("maxWriteToDiskMs",
            Long.toString(maxWriteToDiskMs));
    }
    packetReceiver.close();

    IOException ioe = null;
    if (syncOnClose && (streams.getDataOut() != null || checksumOut != null)) {
      datanode.metrics.incrFsyncCount();
    }
    long flushTotalNanos = 0;
    boolean measuredFlushTime = false;
    // close checksum file
    try {
      if (checksumOut != null) {
        long flushStartNanos = System.nanoTime();
        checksumOut.flush();
        long flushEndNanos = System.nanoTime();
        if (syncOnClose) {
          long fsyncStartNanos = flushEndNanos;
          streams.syncChecksumOut();
          datanode.metrics.addFsyncNanos(System.nanoTime() - fsyncStartNanos);
        }
        flushTotalNanos += flushEndNanos - flushStartNanos;
        measuredFlushTime = true;
        checksumOut.close();
        checksumOut = null;
      }
    } catch(IOException e) {
      ioe = e;
    }
    finally {
      IOUtils.closeStream(checksumOut);
    }
    // close block file
    try {
      if (streams.getDataOut() != null) {
        long flushStartNanos = System.nanoTime();
        streams.flushDataOut();
        long flushEndNanos = System.nanoTime();
        if (syncOnClose) {
          long fsyncStartNanos = flushEndNanos;
          streams.syncDataOut();
          datanode.metrics.addFsyncNanos(System.nanoTime() - fsyncStartNanos);
        }
        flushTotalNanos += flushEndNanos - flushStartNanos;
        measuredFlushTime = true;
        streams.closeDataStream();
      }
    } catch (IOException e) {
      ioe = e;
    }
    finally{
      streams.close();
    }
    if (replicaHandler != null) {
      IOUtils.cleanupWithLogger(null, replicaHandler);
      replicaHandler = null;
    }
    if (measuredFlushTime) {
      datanode.metrics.addFlushNanos(flushTotalNanos);
    }
    if(ioe != null) {
      // Volume error check moved to FileIoProvider
      throw ioe;
    }
  }

  void attachParityHandler(ParityHandler parityHandler) {
    this.parityHandler = parityHandler;
  }

  void setCellSize(int cellSize) {
    this.cellSize = cellSize;
  }

  /**
   * Check if a packet was sent within an acceptable period of time.
   *
   * Some example of when this method may return false:
   * <ul>
   * <li>Upstream did not send packet for a long time</li>
   * <li>Packet was received but got stuck in local disk I/O</li>
   * <li>Packet was received but got stuck on send to mirror</li>
   * </ul>
   *
   * @return true if packet was sent within an acceptable period of time;
   *         otherwise false.
   */
  boolean packetSentInTime() {
    final long diff = Time.monotonicNow() - this.lastSentTime.get();
    final boolean allowedIdleTime = (diff <= this.maxSendIdleTime);
    LOG.debug("A packet was last sent {}ms ago.", diff);
    if (!allowedIdleTime) {
      LOG.warn("A packet was last sent {}ms ago. Maximum idle time: {}ms.",
          diff, this.maxSendIdleTime);
    }
    return allowedIdleTime;
  }

  /**
   * Flush block data and metadata files to disk.
   * @throws IOException
   */
  void flushOrSync(boolean isSync) throws IOException {
    long flushTotalNanos = 0;
    long begin = Time.monotonicNow();
    if (checksumOut != null) {
      long flushStartNanos = System.nanoTime();
      checksumOut.flush();
      long flushEndNanos = System.nanoTime();
      if (isSync) {
        streams.syncChecksumOut();
        datanode.metrics.addFsyncNanos(System.nanoTime() - flushEndNanos);
      }
      flushTotalNanos += flushEndNanos - flushStartNanos;
    }
    if (streams.getDataOut() != null) {
      long flushStartNanos = System.nanoTime();
      streams.flushDataOut();
      long flushEndNanos = System.nanoTime();
      if (isSync) {
        long fsyncStartNanos = flushEndNanos;
        streams.syncDataOut();
        datanode.metrics.addFsyncNanos(System.nanoTime() - fsyncStartNanos);
      }
      flushTotalNanos += flushEndNanos - flushStartNanos;
    }
    if (isSync && !dirSyncOnHSyncDone && replicaInfo instanceof LocalReplica) {
      ((LocalReplica) replicaInfo).fsyncDirectory();
      dirSyncOnHSyncDone = true;
    }
    if (checksumOut != null || streams.getDataOut() != null) {
      datanode.metrics.addFlushNanos(flushTotalNanos);
      if (isSync) {
        datanode.metrics.incrFsyncCount();
      }
    }
    long duration = Time.monotonicNow() - begin;
    if (duration > datanodeSlowLogThresholdMs && LOG.isWarnEnabled()) {
      LOG.warn("Slow flushOrSync took " + duration + "ms (threshold="
          + datanodeSlowLogThresholdMs + "ms), isSync:" + isSync + ", flushTotalNanos="
          + flushTotalNanos + "ns, volume=" + getVolumeBaseUri()
          + ", blockId=" + replicaInfo.getBlockId());
    }
  }

  /**
   * While writing to mirrorOut, failure to write to mirror should not
   * affect this datanode unless it is caused by interruption.
   */
  protected void handleMirrorOutError(IOException ioe) throws IOException {
    String bpid = block.getBlockPoolId();
    LOG.info(datanode.getDNRegistrationForBP(bpid)
        + ":Exception writing " + block + " to mirror " + mirrorAddr, ioe);
    if (Thread.interrupted()) { // shut down if the thread is interrupted
      throw ioe;
    } else { // encounter an error while writing to mirror
      // continue to run even if can not write to mirror
      // notify client of the error
      // and wait for the client to shut down the pipeline
      mirrorError = true;
    }
  }

  /**
   * Verify multiple CRC chunks.
   */
  protected void verifyChunks(ByteBuffer dataBuf, ByteBuffer checksumBuf)
      throws IOException {
    try {
      clientChecksum.verifyChunkedSums(dataBuf, checksumBuf, clientname, 0);
    } catch (ChecksumException ce) {
      PacketHeader header = packetReceiver.getHeader();
      String specificOffset = "specific offsets are:"
          + " offsetInBlock = " + header.getOffsetInBlock()
          + " offsetInPacket = " + ce.getPos();
      LOG.warn("Checksum error in block "
          + block + " from " + inAddr
          + ", " + specificOffset, ce);
      // No need to report to namenode when client is writing.
      if (srcDataNode != null && isDatanode) {
        try {
          LOG.info("report corrupt " + block + " from datanode " +
                    srcDataNode + " to namenode");
          datanode.reportRemoteBadBlock(srcDataNode, block);
        } catch (IOException e) {
          LOG.warn("Failed to report bad " + block +
                    " from datanode " + srcDataNode + " to namenode");
        }
      }
      throw new IOException("Unexpected checksum mismatch while writing "
          + block + " from " + inAddr);
    }
  }


  /**
   * Translate CRC chunks from the client's checksum implementation
   * to the disk checksum implementation.
   *
   * This does not verify the original checksums, under the assumption
   * that they have already been validated.
   */
  protected void translateChunks(ByteBuffer dataBuf, ByteBuffer checksumBuf) {
    diskChecksum.calculateChunkedSums(dataBuf, checksumBuf);
  }

  /**
   * Check whether checksum needs to be verified.
   * Skip verifying checksum iff this is not the last one in the
   * pipeline and clientName is non-null. i.e. Checksum is verified
   * on all the datanodes when the data is being written by a
   * datanode rather than a client. Whe client is writing the data,
   * protocol includes acks and only the last datanode needs to verify
   * checksum.
   * @return true if checksum verification is needed, otherwise false.
   */
  protected boolean shouldVerifyChecksum() {
    return (mirrorOut == null || isDatanode || needsChecksumTranslation);
  }

  /**
   * Receives and processes a packet. It can contain many chunks.
   * returns the number of data bytes that the packet has.
   */
  protected int receivePacket() throws IOException {
    // read the next packet
    packetReceiver.receiveNextPacket(in);

    PacketHeader header = packetReceiver.getHeader();
    if (LOG.isDebugEnabled()){
      LOG.debug("Receiving one packet for block " + block +
                ": " + header);
    }

//    LOG.info("{}, Receiving one packet for block " + block.getBlockId() +
//            ": seqno={}, offset={}, local_offset={}, len={}, lastblock={}, lastinstripe={}", role,
//        header.getSeqno(), header.getOffsetInBlock(),
//        replicaInfo.getNumBytes(),
//        header.getDataLen(),
//        header.isLastPacketInBlock(),
//        header.isLastPacketInStripe());

    if (stripedWrite && (header.getDataLen() + header.getOffsetInBlock()) % cellSize == 0) {
      // last write to this striped node, redirect mirror
      if (++stripeIndex == mirrors.length) {
        stripeIndex = 0;  // loop back to the front
      }
      lastSeqno.offer(new Pair<>(header.getSeqno(), mirrors[stripeIndex].mirrorIn));
    }

    // offset might have been manually modified
    long offsetInBlock;
    if (role == PacketResponderRole.TERMINAL || header.getOffsetInBlock() > replicaInfo.getNumBytes()) {
      offsetInBlock = replicaInfo.getNumBytes();
    } else {
      offsetInBlock = header.getOffsetInBlock();
    }

    if (header.getDataLen() < 0) {
      throw new IOException("Got wrong length during writeBlock(" + block +
                            ") from " + inAddr + " at offset " +
                            header.getOffsetInBlock() + ": " +
                            header.getDataLen());
    }

    long seqno = header.getSeqno();
    boolean lastPacketInBlock = header.isLastPacketInBlock();
    boolean lastPacketInStripe = header.isLastPacketInStripe();
    final int len = header.getDataLen();
    boolean syncBlock = header.getSyncBlock();

    // avoid double sync'ing on close
    if (syncBlock && lastPacketInBlock) {
      this.syncOnClose = false;
      // sync directory for finalize irrespective of syncOnClose config since
      // sync is requested.
      this.dirSyncOnFinalize = true;
    }

    // update received bytes
    final long firstByteInBlock = offsetInBlock;
    offsetInBlock += len;
    if (replicaInfo.getNumBytes() < offsetInBlock) {
      replicaInfo.setNumBytes(offsetInBlock);
    }

    // put in queue for pending acks, unless sync was requested
    if (responder != null && !syncBlock && !shouldVerifyChecksum()) {
      ((PacketResponder) responder.getRunnable()).enqueue(seqno,
          lastPacketInBlock, lastPacketInStripe, offsetInBlock, Status.SUCCESS);
    }

    // Drop heartbeat for testing.
    if (seqno < 0 && len == 0 &&
        DataNodeFaultInjector.get().dropHeartbeatPacket()) {
      return 0;
    }

    //First write the packet to the mirror:
    if (mirrorOut != null && !mirrorError) {
      try {
        long begin = Time.monotonicNow();
        // For testing. Normally no-op.
        DataNodeFaultInjector.get().stopSendingPacketDownstream(mirrorAddr);

        if (stripedWrite && lastPacketInBlock) {
          // if the packet is the block ending packet
          // we need to close out all mirrors
          for (MirrorInfo mirror : mirrors) {
            packetReceiver.mirrorPacketTo(mirror.mirrorOut);
            mirror.mirrorOut.flush();
          }
        } else {
          packetReceiver.mirrorPacketTo(mirrorOut);
          mirrorOut.flush();
        }

        long now = Time.monotonicNow();
        this.lastSentTime.set(now);
        long duration = now - begin;
        DataNodeFaultInjector.get().logDelaySendingPacketDownstream(
            mirrorAddr,
            duration);
        trackSendPacketToLastNodeInPipeline(duration);
        if (duration > datanodeSlowLogThresholdMs && LOG.isWarnEnabled()) {
          LOG.warn("Slow BlockReceiver write packet to mirror took " + duration
              + "ms (threshold=" + datanodeSlowLogThresholdMs + "ms), "
              + "downstream DNs=" + Arrays.toString(downstreamDNs)
              + ", blockId=" + replicaInfo.getBlockId());
        }
      } catch (IOException e) {
        handleMirrorOutError(e);
      }
    }

    ByteBuffer dataBuf = packetReceiver.getDataSlice();
    ByteBuffer checksumBuf = packetReceiver.getChecksumSlice();

    if (lastPacketInBlock || len == 0) {
      if(LOG.isDebugEnabled()) {
        LOG.info("{}, Receiving one packet for block " + block.getBlockId() +
                ": seqno={}, offset={}, local_offset={}, len={}, lastblock={}", role,
            header.getSeqno(), header.getOffsetInBlock(),
            replicaInfo.getNumBytes(),
            header.getDataLen(), header.isLastPacketInBlock());
      }
      // sync block if requested
      if (syncBlock) {
        flushOrSync(true);
      }
    } else {
      final int checksumLen = diskChecksum.getChecksumSize(len);
      final int checksumReceivedLen = checksumBuf.capacity();

      if (checksumReceivedLen > 0 && checksumReceivedLen != checksumLen) {
        throw new IOException("Invalid checksum length: received length is "
            + checksumReceivedLen + " but expected length is " + checksumLen);
      }

      if (checksumReceivedLen > 0 && shouldVerifyChecksum()) {
        try {
          verifyChunks(dataBuf, checksumBuf);
        } catch (IOException ioe) {
          // checksum error detected locally. there is no reason to continue.
          if (responder != null) {
            try {
              ((PacketResponder) responder.getRunnable()).enqueue(seqno,
                  lastPacketInBlock, lastPacketInStripe, offsetInBlock,
                  Status.ERROR_CHECKSUM);
              // Wait until the responder sends back the response
              // and interrupt this thread.
              Thread.sleep(3000);
            } catch (InterruptedException e) { }
          }
          throw new IOException("Terminating due to a checksum error." + ioe);
        }

        if (needsChecksumTranslation) {
          // overwrite the checksums in the packet buffer with the
          // appropriate polynomial for the disk storage.
          translateChunks(dataBuf, checksumBuf);
        }
      }

      if (checksumReceivedLen == 0 && !streams.isTransientStorage()) {
        // checksum is missing, need to calculate it
        checksumBuf = ByteBuffer.allocate(checksumLen);
        diskChecksum.calculateChunkedSums(dataBuf, checksumBuf);
      }

      // by this point, the data in the buffer uses the disk checksum
      final boolean shouldNotWriteChecksum = checksumReceivedLen == 0
          && streams.isTransientStorage();
      try {
        long onDiskLen = replicaInfo.getBytesOnDisk();
        if (onDiskLen<offsetInBlock) {
          // Normally the beginning of an incoming packet is aligned with the
          // existing data on disk. If the beginning packet data offset is not
          // checksum chunk aligned, the end of packet will not go beyond the
          // next chunk boundary.
          // When a failure-recovery is involved, the client state and the
          // the datanode state may not exactly agree. I.e. the client may
          // resend part of data that is already on disk. Correct number of
          // bytes should be skipped when writing the data and checksum
          // buffers out to disk.
          long partialChunkSizeOnDisk = onDiskLen % bytesPerChecksum;
          long lastChunkBoundary = onDiskLen - partialChunkSizeOnDisk;
          boolean alignedOnDisk = partialChunkSizeOnDisk == 0;
          boolean alignedInPacket = firstByteInBlock % bytesPerChecksum == 0;

          // If the end of the on-disk data is not chunk-aligned, the last
          // checksum needs to be overwritten.
          boolean overwriteLastCrc = !alignedOnDisk && !shouldNotWriteChecksum;
          // If the starting offset of the packat data is at the last chunk
          // boundary of the data on disk, the partial checksum recalculation
          // can be skipped and the checksum supplied by the client can be used
          // instead. This reduces disk reads and cpu load.
          boolean doCrcRecalc = overwriteLastCrc &&
              (lastChunkBoundary != firstByteInBlock);

          // If this is a partial chunk, then verify that this is the only
          // chunk in the packet. If the starting offset is not chunk
          // aligned, the packet should terminate at or before the next
          // chunk boundary.
          if (!alignedInPacket && len > bytesPerChecksum) {
            throw new IOException("Unexpected packet data length for "
                +  block + " from " + inAddr + ": a partial chunk must be "
                + " sent in an individual packet (data length = " + len
                +  " > bytesPerChecksum = " + bytesPerChecksum + ")");
          }

          // If the last portion of the block file is not a full chunk,
          // then read in pre-existing partial data chunk and recalculate
          // the checksum so that the checksum calculation can continue
          // from the right state. If the client provided the checksum for
          // the whole chunk, this is not necessary.
          Checksum partialCrc = null;
          if (doCrcRecalc) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("receivePacket for " + block
                  + ": previous write did not end at the chunk boundary."
                  + " onDiskLen=" + onDiskLen);
            }
            long offsetInChecksum = BlockMetadataHeader.getHeaderSize() +
                onDiskLen / bytesPerChecksum * checksumSize;
            partialCrc = computePartialChunkCrc(onDiskLen, offsetInChecksum);
          }

          // The data buffer position where write will begin. If the packet
          // data and on-disk data have no overlap, this will not be at the
          // beginning of the buffer.
          int startByteToDisk = (int)(onDiskLen-firstByteInBlock)
              + dataBuf.arrayOffset() + dataBuf.position();

          // Actual number of data bytes to write.
          int numBytesToDisk = (int)(offsetInBlock-onDiskLen);

          // Write data to disk.
          long begin = Time.monotonicNow();
          streams.writeDataToDisk(dataBuf.array(),
              startByteToDisk, numBytesToDisk);

          // Copy data to async parity handler.
          if (parityHandler != null) {
            parityHandler.writeToParityHandler(dataBuf);
          }
          long duration = Time.monotonicNow() - begin;
          if (duration > datanodeSlowLogThresholdMs && LOG.isWarnEnabled()) {
            LOG.warn("Slow BlockReceiver write data to disk cost:" + duration
                + "ms (threshold=" + datanodeSlowLogThresholdMs + "ms), "
                + "volume=" + getVolumeBaseUri()
                + ", blockId=" + replicaInfo.getBlockId());
          }

          if (duration > maxWriteToDiskMs) {
            maxWriteToDiskMs = duration;
          }

          final byte[] lastCrc;
          if (shouldNotWriteChecksum) {
            lastCrc = null;
          } else {
            int skip = 0;
            byte[] crcBytes = null;

            // First, prepare to overwrite the partial crc at the end.
            if (overwriteLastCrc) { // not chunk-aligned on disk
              // prepare to overwrite last checksum
              adjustCrcFilePosition();
            }

            // The CRC was recalculated for the last partial chunk. Update the
            // CRC by reading the rest of the chunk, then write it out.
            if (doCrcRecalc) {
              // Calculate new crc for this chunk.
              int bytesToReadForRecalc =
                  (int)(bytesPerChecksum - partialChunkSizeOnDisk);
              if (numBytesToDisk < bytesToReadForRecalc) {
                bytesToReadForRecalc = numBytesToDisk;
              }

              partialCrc.update(dataBuf.array(), startByteToDisk,
                  bytesToReadForRecalc);
              byte[] buf = FSOutputSummer.convertToByteStream(partialCrc,
                  checksumSize);
              crcBytes = copyLastChunkChecksum(buf, checksumSize, buf.length);
              checksumOut.write(buf);
              if(LOG.isDebugEnabled()) {
                LOG.debug("Writing out partial crc for data len " + len +
                    ", skip=" + skip);
              }
              skip++; //  For the partial chunk that was just read.
            }

            // Determine how many checksums need to be skipped up to the last
            // boundary. The checksum after the boundary was already counted
            // above. Only count the number of checksums skipped up to the
            // boundary here.
            long skippedDataBytes = lastChunkBoundary - firstByteInBlock;

            if (skippedDataBytes > 0) {
              skip += (int)(skippedDataBytes / bytesPerChecksum) +
                  ((skippedDataBytes % bytesPerChecksum == 0) ? 0 : 1);
            }
            skip *= checksumSize; // Convert to number of bytes

            // write the rest of checksum
            final int offset = checksumBuf.arrayOffset() +
                checksumBuf.position() + skip;
            final int end = offset + checksumLen - skip;
            // If offset >= end, there is no more checksum to write.
            // I.e. a partial chunk checksum rewrite happened and there is no
            // more to write after that.
            if (offset >= end && doCrcRecalc) {
              lastCrc = crcBytes;
            } else {
              final int remainingBytes = checksumLen - skip;
              lastCrc = copyLastChunkChecksum(checksumBuf.array(),
                  checksumSize, end);
              checksumOut.write(checksumBuf.array(), offset, remainingBytes);
            }
          }

          /// flush entire packet, sync if requested
          flushOrSync(syncBlock);

          replicaInfo.setLastChecksumAndDataLen(offsetInBlock, lastCrc);

          datanode.metrics.incrBytesWritten(len);
          datanode.metrics.incrTotalWriteTime(duration);

          manageWriterOsCache(offsetInBlock);
        }
      } catch (IOException iex) {
        // Volume error check moved to FileIoProvider
        throw iex;
      }
    }

    // if sync was requested, put in queue for pending acks here
    // (after the fsync finished)
    if (responder != null && (syncBlock || shouldVerifyChecksum())) {
      ((PacketResponder) responder.getRunnable()).enqueue(seqno,
          lastPacketInBlock, lastPacketInStripe, offsetInBlock, Status.SUCCESS);
    }

    if (stripedWrite && offsetInBlock != 0 && offsetInBlock % cellSize == 0) {
      // last write to this striped node, redirect mirror
      mirrorOut = mirrors[stripeIndex].mirrorOut;
      mirrorAddr = mirrors[stripeIndex].mirrorAddr();
    }

    if (lastPacketInStripe && parityHandler != null) {
      // TODO: notify parityHandler to shutdown since no more packets are comings
      parityHandler.releaseLocks();
    }

    /*
     * Send in-progress responses for the replaceBlock() calls back to caller to
     * avoid timeouts due to balancer throttling. HDFS-6247
     */
    if (isReplaceBlock
        && (Time.monotonicNow() - lastResponseTime > responseInterval)) {
      BlockOpResponseProto.Builder response = BlockOpResponseProto.newBuilder()
          .setStatus(Status.IN_PROGRESS);
      response.build().writeDelimitedTo(replyOut);
      replyOut.flush();

      lastResponseTime = Time.monotonicNow();
    }

    if (throttler != null) { // throttle I/O
      throttler.throttle(len);
    }

    return lastPacketInBlock?-1:len;
  }

  /**
   * Only tracks the latency of sending packet to the last node in pipeline.
   * This is a conscious design choice.
   * <p>
   * In the case of pipeline [dn0, dn1, dn2], 5ms latency from dn0 to dn1, 100ms
   * from dn1 to dn2, NameNode claims dn2 is slow since it sees 100ms latency to
   * dn2. Note that NameNode is not ware of pipeline structure in this context
   * and only sees latency between two DataNodes.
   * </p>
   * <p>
   * In another case of the same pipeline, 100ms latency from dn0 to dn1, 5ms
   * from dn1 to dn2, NameNode will miss detecting dn1 being slow since it's not
   * the last node. However the assumption is that in a busy enough cluster
   * there are many other pipelines where dn1 is the last node, e.g. [dn3, dn4,
   * dn1]. Also our tracking interval is relatively long enough (at least an
   * hour) to improve the chances of the bad DataNodes being the last nodes in
   * multiple pipelines.
   * </p>
   */
  protected void trackSendPacketToLastNodeInPipeline(final long elapsedMs) {
    final DataNodePeerMetrics peerMetrics = datanode.getPeerMetrics();
    if (peerMetrics != null && isPenultimateNode) {
      peerMetrics.addSendPacketDownstream(mirrorNameForMetrics, elapsedMs);
    }
  }

  protected static byte[] copyLastChunkChecksum(byte[] array, int size, int end) {
    return Arrays.copyOfRange(array, end - size, end);
  }

  protected void manageWriterOsCache(long offsetInBlock) {
    try {
      if (streams.getOutFd() != null &&
          offsetInBlock > lastCacheManagementOffset + CACHE_DROP_LAG_BYTES) {
        long begin = Time.monotonicNow();
        //
        // For SYNC_FILE_RANGE_WRITE, we want to sync from
        // lastCacheManagementOffset to a position "two windows ago"
        //
        //                         <========= sync ===========>
        // +-----------------------O--------------------------X
        // start                  last                      curPos
        // of file
        //
        if (syncBehindWrites) {
          if (syncBehindWritesInBackground) {
            this.datanode.getFSDataset().submitBackgroundSyncFileRangeRequest(
                block, streams, lastCacheManagementOffset,
                offsetInBlock - lastCacheManagementOffset,
                SYNC_FILE_RANGE_WRITE);
          } else {
            streams.syncFileRangeIfPossible(lastCacheManagementOffset,
                offsetInBlock - lastCacheManagementOffset,
                SYNC_FILE_RANGE_WRITE);
          }
        }
        //
        // For POSIX_FADV_DONTNEED, we want to drop from the beginning
        // of the file to a position prior to the current position.
        //
        // <=== drop =====>
        //                 <---W--->
        // +--------------+--------O--------------------------X
        // start        dropPos   last                      curPos
        // of file
        //
        long dropPos = lastCacheManagementOffset - CACHE_DROP_LAG_BYTES;
        if (dropPos > 0 && dropCacheBehindWrites) {
          streams.dropCacheBehindWrites(block.getBlockName(), 0, dropPos,
              POSIX_FADV_DONTNEED);
        }
        lastCacheManagementOffset = offsetInBlock;
        long duration = Time.monotonicNow() - begin;
        if (duration > datanodeSlowLogThresholdMs && LOG.isWarnEnabled()) {
          LOG.warn("Slow manageWriterOsCache took " + duration
              + "ms (threshold=" + datanodeSlowLogThresholdMs
              + "ms), volume=" + getVolumeBaseUri()
              + ", blockId=" + replicaInfo.getBlockId());
        }
      }
    } catch (Throwable t) {
      LOG.warn("Error managing cache for writer of block " + block, t);
    }
  }

  public void sendOOB() throws IOException, InterruptedException {
    if (isDatanode) {
      return;
    }
    ((PacketResponder) responder.getRunnable()).sendOOBResponse(PipelineAck
        .getRestartOOBStatus());
  }

  public static class MirrorInfo {
    DataOutputStream mirrorOut = null;  // stream to next target
    DataInputStream mirrorIn = null;    // reply from next target
    Socket mirrorSock = null;           // socket to next target
    String mirrorNode = null;           // the name:port of next target

    String mirrorAddr() {
      return (mirrorSock == null) ? null : mirrorNode;
    }
  }

  void receiveHybridBlock(
      MirrorInfo[] mirrors,
      DataOutputStream replyOut,
      Long[] offsets,
      DataTransferThrottler throttlerArg,
      DatanodeInfo[] downstreams,
      boolean isReplaceBlock) throws IOException {

    syncOnClose = datanode.getDnConf().syncOnClose;
    dirSyncOnFinalize = syncOnClose;
    boolean responderClosed = false;

    // save the new mirrors in class field,
    // receivePacket will use this.mirrors to determine the rotation if it's set
    this.mirrors = mirrors;
    this.stripedWrite = true;
    this.role = PacketResponderRole.STRIPER;
    this.lastSeqno = new ConcurrentLinkedDeque<>();

    // when number decreases there is boundary
    this.stripeIndex = 0;
    for (int i = 1; i < offsets.length; i++) {
      if (offsets[i] < offsets[i - 1]) {
        // start writing from the lowest left-most value
        this.stripeIndex = i;
        break;
      }
    }

    this.mirrorOut = mirrors[stripeIndex].mirrorOut;
    this.mirrorAddr = mirrors[stripeIndex].mirrorAddr();
    initPerfMonitoring(downstreams);
    this.throttler = throttlerArg;

    this.replyOut = replyOut;
    this.isReplaceBlock = isReplaceBlock;

    final DataInputStream[] mirrIns = new DataInputStream[mirrors.length];
    for (int i = 0; i < mirrors.length; i++) {
      mirrIns[i] = mirrors[i].mirrorIn;
    }

    try {
      if (isClient && !isTransfer) {
        responder = new Daemon(datanode.threadGroup,
                new PacketResponder(
                        replyOut, mirrIns[stripeIndex],
                        mirrIns, downstreams));
        responder.start(); // start thread to processes responses
      }
      final long startTime = System.currentTimeMillis();

      while (receivePacket() >= 0) { /* Receive until the last packet */ }

      LOG.debug("{}, Closing out hybrid packet receiver, took {} ms to receive packets fully",
          role, System.currentTimeMillis() - startTime);
      // wait for all outstanding packet responses. And then
      // indicate responder to gracefully shutdown.
      // Mark that responder has been closed for future processing
      if (responder != null) {
        ((PacketResponder)responder.getRunnable()).close();
        responderClosed = true;
      }

      // If this write is for a replication or transfer-RBW/Finalized,
      // then finalize block or convert temporary to RBW.
      // For client-writes, the block is finalized in the PacketResponder.
      if (isDatanode || isTransfer) {
        // Hold a volume reference to finalize block.
        try (ReplicaHandler handler = claimReplicaHandler()) {
          // close the block/crc files
          close();
          block.setNumBytes(replicaInfo.getNumBytes());

          if (stage == BlockConstructionStage.TRANSFER_RBW) {
            // for TRANSFER_RBW, convert temporary to RBW
            datanode.data.convertTemporaryToRbw(block);
          } else {
            // for isDatnode or TRANSFER_FINALIZED
            // Finalize the block.
            datanode.data.finalizeBlock(block, dirSyncOnFinalize);
          }
        }
        datanode.metrics.incrBlocksWritten();
      }

    } catch (IOException ioe) {
      replicaInfo.releaseAllBytesReserved();
      if (datanode.isRestarting()) {
        // Do not throw if shutting down for restart. Otherwise, it will cause
        // premature termination of responder.
        LOG.info("Shutting down for restart (" + block + ").");
      } else {
        LOG.error("Exception for hybrid block " + block, ioe);
        throw ioe;
      }
    } finally {
      // Clear the previous interrupt state of this thread.
      Thread.interrupted();

      // If a shutdown for restart was initiated, upstream needs to be notified.
      // There is no need to do anything special if the responder was closed
      // normally.
      if (!responderClosed) { // Data transfer was not complete.
        if (responder != null) {
          // In case this datanode is shutting down for quick restart,
          // send a special ack upstream.
          if (datanode.isRestarting() && isClient && !isTransfer) {
            try (Writer out = new OutputStreamWriter(
                    replicaInfo.createRestartMetaStream(), "UTF-8")) {
              // write out the current time.
              out.write(Long.toString(Time.now() + restartBudget));
              out.flush();
            } catch (IOException ioe) {
              // The worst case is not recovering this RBW replica.
              // Client will fall back to regular pipeline recovery.
            } finally {
              IOUtils.closeStream(streams.getDataOut());
            }
            try {
              // Even if the connection is closed after the ack packet is
              // flushed, the client can react to the connection closure
              // first. Insert a delay to lower the chance of client
              // missing the OOB ack.
              Thread.sleep(1000);
            } catch (InterruptedException ie) {
              // It is already going down. Ignore this.
            }
          }
          responder.interrupt();
        }
        IOUtils.closeStream(this);
        cleanupBlock();
      }
      if (responder != null) {
        try {
          responder.interrupt();
          // join() on the responder should timeout a bit earlier than the
          // configured deadline. Otherwise, the join() on this thread will
          // likely timeout as well.
          long joinTimeout = datanode.getDnConf().getXceiverStopTimeout();
          joinTimeout = joinTimeout > 1  ? joinTimeout*8/10 : joinTimeout;
          responder.join(joinTimeout);
          if (responder.isAlive()) {
            String msg = "Join on responder thread " + responder
                    + " timed out";
            LOG.warn(msg + "\n" + StringUtils.getStackTrace(responder));
            throw new IOException(msg);
          }
        } catch (InterruptedException e) {
          responder.interrupt();
          // do not throw if shutting down for restart.
          if (!datanode.isRestarting()) {
            throw new InterruptedIOException("Interrupted receiveBlock");
          }
        }
        responder = null;
      }
    }
  }

  void receiveBlock(
      DataOutputStream mirrOut, // output to next datanode
      DataInputStream mirrIn,   // input from next datanode
      DataOutputStream replyOut,  // output to previous datanode
      String mirrAddr, DataTransferThrottler throttlerArg,
      DatanodeInfo[] downstreams,
      boolean isReplaceBlock) throws IOException {

    syncOnClose = datanode.getDnConf().syncOnClose;
    dirSyncOnFinalize = syncOnClose;
    boolean responderClosed = false;
    mirrorOut = mirrOut;
    mirrorAddr = mirrAddr;
    initPerfMonitoring(downstreams);
    throttler = throttlerArg;

    if (mirrOut != null) {
      this.role = PacketResponderRole.MIRROR;
    } else {
      this.role = PacketResponderRole.TERMINAL;
    }

    this.replyOut = replyOut;
    this.isReplaceBlock = isReplaceBlock;

    try {
      if (isClient && !isTransfer) {
        responder = new Daemon(datanode.threadGroup,
            new PacketResponder(replyOut, mirrIn, downstreams));
        responder.start(); // start thread to processes responses
      }
      final long startTime = System.currentTimeMillis();

      while (receivePacket() >= 0) { /* Receive until the last packet */ }

      LOG.debug("{}, Closing out normal packet receiver, took {} ms to receive packets fully",
          role, System.currentTimeMillis() - startTime);
      // wait for all outstanding packet responses. And then
      // indicate responder to gracefully shutdown.
      // Mark that responder has been closed for future processing
      if (responder != null) {
        ((PacketResponder)responder.getRunnable()).close();
        responderClosed = true;
      }

      // If this write is for a replication or transfer-RBW/Finalized,
      // then finalize block or convert temporary to RBW.
      // For client-writes, the block is finalized in the PacketResponder.
      if (isDatanode || isTransfer) {
        // Hold a volume reference to finalize block.
        try (ReplicaHandler handler = claimReplicaHandler()) {
          // close the block/crc files
          close();
          block.setNumBytes(replicaInfo.getNumBytes());

          if (stage == BlockConstructionStage.TRANSFER_RBW) {
            // for TRANSFER_RBW, convert temporary to RBW
            datanode.data.convertTemporaryToRbw(block);
          } else {
            // for isDatnode or TRANSFER_FINALIZED
            // Finalize the block.
            datanode.data.finalizeBlock(block, dirSyncOnFinalize);
          }
        }
        datanode.metrics.incrBlocksWritten();
      }

    } catch (IOException ioe) {
      replicaInfo.releaseAllBytesReserved();
      if (datanode.isRestarting()) {
        // Do not throw if shutting down for restart. Otherwise, it will cause
        // premature termination of responder.
        LOG.info("Shutting down for restart (" + block + ").");
      } else {
        LOG.error("Exception for " + block, ioe);
        throw ioe;
      }
    } finally {
      // Clear the previous interrupt state of this thread.
      Thread.interrupted();

      // If a shutdown for restart was initiated, upstream needs to be notified.
      // There is no need to do anything special if the responder was closed
      // normally.
      if (!responderClosed) { // Data transfer was not complete.
        if (responder != null) {
          // In case this datanode is shutting down for quick restart,
          // send a special ack upstream.
          if (datanode.isRestarting() && isClient && !isTransfer) {
            try (Writer out = new OutputStreamWriter(
                replicaInfo.createRestartMetaStream(), "UTF-8")) {
              // write out the current time.
              out.write(Long.toString(Time.now() + restartBudget));
              out.flush();
            } catch (IOException ioe) {
              // The worst case is not recovering this RBW replica.
              // Client will fall back to regular pipeline recovery.
            } finally {
              IOUtils.closeStream(streams.getDataOut());
            }
            try {
              // Even if the connection is closed after the ack packet is
              // flushed, the client can react to the connection closure
              // first. Insert a delay to lower the chance of client
              // missing the OOB ack.
              Thread.sleep(1000);
            } catch (InterruptedException ie) {
              // It is already going down. Ignore this.
            }
          }
          responder.interrupt();
        }
        IOUtils.closeStream(this);
        cleanupBlock();
      }
      if (responder != null) {
        try {
          responder.interrupt();
          // join() on the responder should timeout a bit earlier than the
          // configured deadline. Otherwise, the join() on this thread will
          // likely timeout as well.
          long joinTimeout = datanode.getDnConf().getXceiverStopTimeout();
          joinTimeout = joinTimeout > 1  ? joinTimeout*8/10 : joinTimeout;
          responder.join(joinTimeout);
          if (responder.isAlive()) {
            String msg = "Join on responder thread " + responder
                + " timed out";
            LOG.warn(msg + "\n" + StringUtils.getStackTrace(responder));
            throw new IOException(msg);
          }
        } catch (InterruptedException e) {
          responder.interrupt();
          // do not throw if shutting down for restart.
          if (!datanode.isRestarting()) {
            throw new InterruptedIOException("Interrupted receiveBlock");
          }
        }
        responder = null;
      }
    }
  }

  /**
   * If we have downstream DNs and peerMetrics are enabled, then initialize
   * some state for monitoring the performance of downstream DNs.
   *
   * @param downstreams downstream DNs, or null if there are none.
   */
  protected void initPerfMonitoring(DatanodeInfo[] downstreams) {
    if (downstreams != null && downstreams.length > 0) {
      downstreamDNs = downstreams;
      isPenultimateNode = (downstreams.length == 1);
      if (isPenultimateNode && datanode.getPeerMetrics() != null) {
        mirrorNameForMetrics = (downstreams[0].getInfoSecurePort() != 0 ?
            downstreams[0].getInfoSecureAddr() : downstreams[0].getInfoAddr());
        LOG.debug("Will collect peer metrics for downstream node {}",
            mirrorNameForMetrics);
      }
    }
  }

  /**
   * Fetch the base URI of the volume on which this replica resides.
   *
   * @returns Volume base URI as string if available. Else returns the
   *          the string "unavailable".
   */
  protected String getVolumeBaseUri() {
    final ReplicaInfo ri = replicaInfo.getReplicaInfo();
    if (ri != null && ri.getVolume() != null) {
      return ri.getVolume().getBaseURI().toString();
    }
    return "unavailable";
  }

  /** Cleanup a partial block
   * if this write is for a replication request (and not from a client)
   */
  protected void cleanupBlock() throws IOException {
    if (isDatanode) {
      datanode.data.unfinalizeBlock(block);
    }
  }

  /**
   * Adjust the file pointer in the local meta file so that the last checksum
   * will be overwritten.
   */
  protected void adjustCrcFilePosition() throws IOException {
    streams.flushDataOut();
    if (checksumOut != null) {
      checksumOut.flush();
    }

    // rollback the position of the meta file
    datanode.data.adjustCrcChannelPosition(block, streams, checksumSize);
  }

  /**
   * Convert a checksum byte array to a long
   */
  static protected long checksum2long(byte[] checksum) {
    long crc = 0L;
    for(int i=0; i<checksum.length; i++) {
      crc |= (0xffL&checksum[i])<<((checksum.length-i-1)*8);
    }
    return crc;
  }

  /**
   * reads in the partial crc chunk and computes checksum
   * of pre-existing data in partial chunk.
   */
  protected Checksum computePartialChunkCrc(long blkoff, long ckoff)
      throws IOException {

    // find offset of the beginning of partial chunk.
    //
    int sizePartialChunk = (int) (blkoff % bytesPerChecksum);
    blkoff = blkoff - sizePartialChunk;
    if (LOG.isDebugEnabled()) {
      LOG.debug("computePartialChunkCrc for " + block
          + ": sizePartialChunk=" + sizePartialChunk
          + ", block offset=" + blkoff
          + ", metafile offset=" + ckoff);
    }

    // create an input stream from the block file
    // and read in partial crc chunk into temporary buffer
    //
    byte[] buf = new byte[sizePartialChunk];
    byte[] crcbuf = new byte[checksumSize];
    try (ReplicaInputStreams instr =
        datanode.data.getTmpInputStreams(block, blkoff, ckoff)) {
      instr.readDataFully(buf, 0, sizePartialChunk);

      // open meta file and read in crc value computer earlier
      instr.readChecksumFully(crcbuf, 0, crcbuf.length);
    }

    // compute crc of partial chunk from data read in the block file.
    final Checksum partialCrc = DataChecksum.newDataChecksum(
        diskChecksum.getChecksumType(), diskChecksum.getBytesPerChecksum());
    partialCrc.update(buf, 0, sizePartialChunk);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Read in partial CRC chunk from disk for " + block);
    }

    // paranoia! verify that the pre-computed crc matches what we
    // recalculated just now
    if (partialCrc.getValue() != checksum2long(crcbuf)) {
      String msg = "Partial CRC " + partialCrc.getValue() +
                   " does not match value computed the " +
                   " last time file was closed " +
                   checksum2long(crcbuf);
      throw new IOException(msg);
    }
    return partialCrc;
  }

  /** The caller claims the ownership of the replica handler. */
  protected ReplicaHandler claimReplicaHandler() {
    ReplicaHandler handler = replicaHandler;
    replicaHandler = null;
    return handler;
  }

  protected enum PacketResponderType {
    NON_PIPELINE, LAST_IN_PIPELINE, HAS_DOWNSTREAM_IN_PIPELINE
  }

  protected enum PacketResponderRole {
    MIRROR, STRIPER, TERMINAL
  }

  /**
   * Processes responses from downstream datanodes in the pipeline
   * and sends back replies to the originator.
   */
  class PacketResponder implements Runnable, Closeable {
    /** queue for packets waiting for ack - synchronization using monitor lock */
    protected final Queue<Packet> ackQueue = new ArrayDeque<>();
    /** the thread that spawns this responder */
    protected final Thread receiverThread = Thread.currentThread();
    /** is this responder running? - synchronization using monitor lock */
    protected volatile boolean running = true;
    /** inputs from the downstream nodes (necessary when striping)*/
    protected DataInputStream[] downstreamIns;
    /** input from the next downstream datanode */
    protected DataInputStream downstreamIn;
    /** output to upstream datanode/client */
    protected final DataOutputStream upstreamOut;
    /** The type of this responder */
    protected final PacketResponderType type;

    protected PacketResponderRole role;
    /** for log and error messages */
    protected final String myString;
    protected boolean sending = false;

    @Override
    public String toString() {
      return myString;
    }

    PacketResponder(final DataOutputStream upstreamOut,
        final DataInputStream downstreamIn, final DatanodeInfo[] downstreams) {
      PacketResponderType rtype;
      this.downstreamIn = downstreamIn;
      this.upstreamOut = upstreamOut;

      if (downstreamIn != null) {
        this.role = PacketResponderRole.MIRROR;
        rtype = PacketResponderType.HAS_DOWNSTREAM_IN_PIPELINE;
      } else {
        this.role = PacketResponderRole.TERMINAL;
        rtype =  PacketResponderType.LAST_IN_PIPELINE;
      }
      if (downstreams == null) {
        this.role = PacketResponderRole.TERMINAL;
        rtype =  PacketResponderType.NON_PIPELINE;
      }

      this.type = rtype;
      final StringBuilder b = new StringBuilder(getClass().getSimpleName())
          .append(": ").append(block).append(", type=").append(type);
      if (type == PacketResponderType.HAS_DOWNSTREAM_IN_PIPELINE) {
        b.append(", downstreams=").append(downstreams.length)
            .append(":").append(Arrays.asList(downstreams));
      }
      this.myString = b.toString();
    }

    PacketResponder(final DataOutputStream upstreamOut, final DataInputStream firstDownstreamIn,
        final DataInputStream[] downstreamIns, final DatanodeInfo[] downstreams) {
      this(upstreamOut, firstDownstreamIn, downstreams);
      this.downstreamIns = downstreamIns;
      this.role = PacketResponderRole.STRIPER;
    }

    protected boolean isRunning() {
      // When preparing for a restart, it should continue to run until
      // interrupted by the receiver thread.
      return running && (datanode.shouldRun || datanode.isRestarting());
    }

    void shiftDownstream(int newindex) {
      this.downstreamIn = this.downstreamIns[newindex];
    }

    /**
     * enqueue the seqno that is still be to acked by the downstream datanode.
     *
     * @param seqno              sequence number of the packet
     * @param lastPacketInBlock  if true, this is the last packet in block
     * @param lastPacketInStripe
     * @param offsetInBlock      offset of this packet in block
     */
    void enqueue(final long seqno, final boolean lastPacketInBlock, boolean lastPacketInStripe,
                 final long offsetInBlock, final Status ackStatus) {
      final Packet p = new Packet(seqno, lastPacketInBlock,
          lastPacketInStripe, offsetInBlock, System.nanoTime(), ackStatus);
      synchronized (ackQueue) {
        if (running) {
          ackQueue.add(p);
          ackQueue.notifyAll();
        }
      }
    }

    /**
     * Send an OOB response. If all acks have been sent already for the block
     * and the responder is about to close, the delivery is not guaranteed.
     * This is because the other end can close the connection independently.
     * An OOB coming from downstream will be automatically relayed upstream
     * by the responder. This method is used only by originating datanode.
     *
     * @param ackStatus the type of ack to be sent
     */
    void sendOOBResponse(final Status ackStatus) throws IOException,
        InterruptedException {
      if (!running) {
        LOG.info("Cannot send OOB response " + ackStatus +
            ". Responder not running.");
        return;
      }

      synchronized(this) {
        if (sending) {
          wait(datanode.getOOBTimeout(ackStatus));
          // Didn't get my turn in time. Give up.
          if (sending) {
            throw new IOException("Could not send OOB reponse in time: "
                + ackStatus);
          }
        }
        sending = true;
      }

      LOG.info("Sending an out of band ack of type " + ackStatus);
      try {
        sendAckUpstreamUnprotected(null, PipelineAck.UNKOWN_SEQNO, 0L, 0L,
            PipelineAck.combineHeader(datanode.getECN(), ackStatus));
      } finally {
        // Let others send ack. Unless there are miltiple OOB send
        // calls, there can be only one waiter, the responder thread.
        // In any case, only one needs to be notified.
        synchronized(this) {
          sending = false;
          notify();
        }
      }
    }

    /** Wait for a packet with given {@code seqno} to be enqueued to ackQueue */
    Packet waitForAckHead(long seqno) throws InterruptedException {
      synchronized (ackQueue) {
        while (isRunning() && ackQueue.isEmpty()) {
          LOG.debug("{}: seqno={} waiting for local datanode to finish write.",
              myString, seqno);
          ackQueue.wait();
        }
        return isRunning() ? ackQueue.element() : null;
      }
    }

    /**
     * wait for all pending packets to be acked. Then shutdown thread.
     */
    @Override
    public void close() {
      synchronized (ackQueue) {
        while (isRunning() && !ackQueue.isEmpty()) {
          try {
            ackQueue.wait();
          } catch (InterruptedException e) {
            running = false;
            Thread.currentThread().interrupt();
          }
        }
        LOG.debug("{}: closing", this);
        running = false;
        ackQueue.notifyAll();
      }

      synchronized (this) {
        running = false;
        notifyAll();
      }
    }

    /**
     * Thread to process incoming acks.
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
      datanode.metrics.incrDataNodePacketResponderCount();
      boolean lastPacketInBlock = false;
      boolean lastPacketInStripe = false;
      final long startTime = ClientTraceLog.isInfoEnabled() ? System.nanoTime() : 0;
      while (isRunning() && !lastPacketInBlock) {
        long totalAckTimeNanos = 0;
        boolean isInterrupted = false;
        try {
          Packet pkt = null;
          long expected = -2;
          PipelineAck ack = new PipelineAck();
          long seqno = PipelineAck.UNKOWN_SEQNO;
          long ackRecvNanoTime = 0;
          try {
            if (type != PacketResponderType.LAST_IN_PIPELINE && !mirrorError) {
              DataNodeFaultInjector.get().failPipeline(replicaInfo, mirrorAddr);
              // read an ack from downstream datanode
              ack.readFields(downstreamIn);
              ackRecvNanoTime = System.nanoTime();
              if (LOG.isDebugEnabled()) {
                LOG.debug(myString + " got " + ack);
              }
              // Process an OOB ACK.
              Status oobStatus = ack.getOOBStatus();
              if (oobStatus != null) {
                LOG.info("Relaying an out of band ack of type " + oobStatus);
                sendAckUpstream(ack, PipelineAck.UNKOWN_SEQNO, 0L, 0L,
                    PipelineAck.combineHeader(datanode.getECN(),
                      Status.SUCCESS));
                continue;
              }
              seqno = ack.getSeqno();
              // LOG.info("{}: Received an ack from downstream {}, size of lastseq {}", role, seqno, lastSeqno.size());

              // issue is, downstreamIn gets rotated before we're done reading from it
              // therefore, it is important to keep a queue of which ins are ready to be read from
              // we can track that because we know which packets we have sent out already
              if (stripedWrite && !lastSeqno.isEmpty()) {
                if (lastSeqno.peekFirst().getKey() == seqno) {
                  downstreamIn = lastSeqno.pollFirst().getValue();
                } else {
                  // LOG.warn("Peeked last seq no and it was {} but was expecting {}", lastSeqno.peekFirst().getKey(), seqno);
                }
                // LOG.info("{}: Rotating input stream! based on seqno {}", role, seqno);
                // if seqno is the last seqno for this stream
                // then rotate to the new input stream for the next cycle
              }
            }
            if (seqno != PipelineAck.UNKOWN_SEQNO
                || type == PacketResponderType.LAST_IN_PIPELINE) {
              pkt = waitForAckHead(seqno);
              if (!isRunning()) {
                break;
              }
              expected = pkt.seqno;
              if (type == PacketResponderType.HAS_DOWNSTREAM_IN_PIPELINE
                  && seqno != expected) {
                throw new IOException(myString + "seqno: expected=" + expected
                    + ", received=" + seqno);
              }
              if (type == PacketResponderType.HAS_DOWNSTREAM_IN_PIPELINE) {
                // The total ack time includes the ack times of downstream
                // nodes.
                // The value is 0 if this responder doesn't have a downstream
                // DN in the pipeline.
                totalAckTimeNanos = ackRecvNanoTime - pkt.ackEnqueueNanoTime;
                // Report the elapsed time from ack send to ack receive minus
                // the downstream ack time.
                long ackTimeNanos = totalAckTimeNanos
                    - ack.getDownstreamAckTimeNanos();
                if (ackTimeNanos < 0) {
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("Calculated invalid ack time: " + ackTimeNanos
                        + "ns.");
                  }
                } else {
                  datanode.metrics.addPacketAckRoundTripTimeNanos(ackTimeNanos);
                }
              }
              lastPacketInBlock = pkt.lastPacketInBlock;
              lastPacketInStripe = pkt.lastPacketInStripe;
            }
          } catch (InterruptedException ine) {
            isInterrupted = true;
          } catch (IOException ioe) {
            if (Thread.interrupted()) {
              isInterrupted = true;
            } else if (ioe instanceof EOFException && !packetSentInTime()) {
              // The downstream error was caused by upstream including this
              // node not sending packet in time. Let the upstream determine
              // who is at fault.  If the immediate upstream node thinks it
              // has sent a packet in time, this node will be reported as bad.
              // Otherwise, the upstream node will propagate the error up by
              // closing the connection.
              LOG.warn("The downstream error might be due to congestion in " +
                  "upstream including this node. Propagating the error: ",
                  ioe);
              throw ioe;
            } else {
              // continue to run even if can not read from mirror
              // notify client of the error
              // and wait for the client to shut down the pipeline
              mirrorError = true;
              LOG.info(myString, ioe);
            }
          }

          if (Thread.interrupted() || isInterrupted) {
            /*
             * The receiver thread cancelled this thread. We could also check
             * any other status updates from the receiver thread (e.g. if it is
             * ok to write to replyOut). It is prudent to not send any more
             * status back to the client because this datanode has a problem.
             * The upstream datanode will detect that this datanode is bad, and
             * rightly so.
             *
             * The receiver thread can also interrupt this thread for sending
             * an out-of-band response upstream.
             */
            LOG.info(myString + ": Thread is interrupted.");
            running = false;
            continue;
          }

          if (lastPacketInBlock) {
            // Finalize the block and close the block file
            // we should also finalize even when terminal when blocksize=cellsize
            final boolean isContiguous = cellSize == 1024 * 1024 * 8;
            final boolean shouldComplete = isContiguous || role != PacketResponderRole.TERMINAL || lastPacketInStripe;
            finalizeBlock(startTime, shouldComplete);
          }

          Status myStatus = pkt != null ? pkt.ackStatus : Status.SUCCESS;
          sendAckUpstream(ack, expected, totalAckTimeNanos,
            (pkt != null ? pkt.offsetInBlock : 0),
            PipelineAck.combineHeader(datanode.getECN(), myStatus));

          if (pkt != null) {
            // remove the packet from the ack queue
            removeAckHead();
          }
        } catch (IOException e) {
          LOG.warn("IOException in PacketResponder.run(): ", e);
          if (running) {
            // Volume error check moved to FileIoProvider
            LOG.info(myString, e);
            running = false;
            if (!Thread.interrupted()) { // failure not caused by interruption
              receiverThread.interrupt();
            }
          }
        } catch (Throwable e) {
          if (running) {
            LOG.info(myString, e);
            running = false;
            receiverThread.interrupt();
          }
        }
      }
      // Any exception will be caught and processed in the previous loop, so we
      // will always arrive here when the thread exiting
      datanode.metrics.decrDataNodePacketResponderCount();
      LOG.debug(myString + " terminating");
    }

    /**
     * Finalize the block and close the block file
     * @param startTime time when BlockReceiver started receiving the block
     * @param close a flag that distinguishes if this block is truly complete
     *                 or will soon be appended to by another write. if complete
     *                 is set to false, the block will be kept cached at the
     *                 datanode, waiting for someone to append to or close it.
     */
    protected void finalizeBlock(long startTime, boolean close) throws IOException {
      long endTime = 0;
      // Hold a volume reference to finalize block.
      try (ReplicaHandler handler = BlockReceiver.this.claimReplicaHandler()) {
        BlockReceiver.this.close();
        endTime = ClientTraceLog.isInfoEnabled() ? System.nanoTime() : 0;
        block.setNumBytes(replicaInfo.getNumBytes());
        if (close) {
          // keep replica in rbw state for further writing
          datanode.data.finalizeBlock(block, dirSyncOnFinalize);
        }
      }

      if (pinning) {
        datanode.data.setPinning(block);
      }

      if (close) {
        // don't notify namenode until the block is fully complete
        datanode.closeBlock(block, null, replicaInfo.getStorageUuid(),
            replicaInfo.isOnTransientStorage());
      }
      if (ClientTraceLog.isInfoEnabled() && isClient) {
        long offset = 0;
        DatanodeRegistration dnR = datanode.getDNRegistrationForBP(block
            .getBlockPoolId());
        ClientTraceLog.info(String.format(DN_CLIENTTRACE_FORMAT, inAddr,
            myAddr, block.getNumBytes(), "HDFS_WRITE", clientname, offset,
            dnR.getDatanodeUuid(), block, endTime - startTime));
      } else {
        LOG.info("Received " + block + " size " + block.getNumBytes()
            + " from " + inAddr);
      }
    }

    /**
     * The wrapper for the unprotected version. This is only called by
     * the responder's run() method.
     *
     * @param ack Ack received from downstream
     * @param seqno sequence number of ack to be sent upstream
     * @param totalAckTimeNanos total ack time including all the downstream
     *          nodes
     * @param offsetInBlock offset in block for the data in packet
     * @param myHeader the local ack header
     */
    protected void sendAckUpstream(PipelineAck ack, long seqno,
        long totalAckTimeNanos, long offsetInBlock,
        int myHeader) throws IOException {
      try {
        // Wait for other sender to finish. Unless there is an OOB being sent,
        // the responder won't have to wait.
        synchronized(this) {
          while(sending) {
            wait();
          }
          sending = true;
        }

        try {
          if (!running) return;
          sendAckUpstreamUnprotected(ack, seqno, totalAckTimeNanos,
              offsetInBlock, myHeader);
        } finally {
          synchronized(this) {
            sending = false;
            notify();
          }
        }
      } catch (InterruptedException ie) {
        // The responder was interrupted. Make it go down without
        // interrupting the receiver(writer) thread.
        running = false;
      }
    }

    /**
     * @param ack Ack received from downstream
     * @param seqno sequence number of ack to be sent upstream
     * @param totalAckTimeNanos total ack time including all the downstream
     *          nodes
     * @param offsetInBlock offset in block for the data in packet
     * @param myHeader the local ack header
     */
    protected void sendAckUpstreamUnprotected(PipelineAck ack, long seqno,
        long totalAckTimeNanos, long offsetInBlock, int myHeader)
        throws IOException {
      final int[] replies;
      if (ack == null) {
        // A new OOB response is being sent from this node. Regardless of
        // downstream nodes, reply should contain one reply.
        replies = new int[] { myHeader };
      } else if (mirrorError) { // ack read error
        int h = PipelineAck.combineHeader(datanode.getECN(), Status.SUCCESS);
        int h1 = PipelineAck.combineHeader(datanode.getECN(), Status.ERROR);
        replies = new int[] {h, h1};
      } else {
        short ackLen = type == PacketResponderType.LAST_IN_PIPELINE ? 0 : ack
            .getNumOfReplies();
        replies = new int[ackLen + 1];
        replies[0] = myHeader;
        for (int i = 0; i < ackLen; ++i) {
          replies[i + 1] = ack.getHeaderFlag(i);
        }
        // If the mirror has reported that it received a corrupt packet,
        // do self-destruct to mark myself bad, instead of making the
        // mirror node bad. The mirror is guaranteed to be good without
        // corrupt data on disk.
        if (ackLen > 0 && PipelineAck.getStatusFromHeader(replies[1]) ==
          Status.ERROR_CHECKSUM) {
          throw new IOException("Shutting down writer and responder "
              + "since the down streams reported the data sent by this "
              + "thread is corrupt");
        }
      }
      PipelineAck replyAck = new PipelineAck(seqno, replies,
          totalAckTimeNanos);
      if (replyAck.isSuccess()
          && offsetInBlock > replicaInfo.getBytesAcked()) {
        replicaInfo.setBytesAcked(offsetInBlock);
      }
      // send my ack back to upstream datanode
      long begin = Time.monotonicNow();
      /* for test only, no-op in production system */
      DataNodeFaultInjector.get().delaySendingAckToUpstream(inAddr);
      replyAck.write(upstreamOut);
      upstreamOut.flush();
      long duration = Time.monotonicNow() - begin;
      DataNodeFaultInjector.get().logDelaySendingAckToUpstream(
          inAddr,
          duration);
      if (duration > datanodeSlowLogThresholdMs) {
        LOG.warn("Slow PacketResponder send ack to upstream took " + duration
            + "ms (threshold=" + datanodeSlowLogThresholdMs + "ms), " + myString
            + ", replyAck=" + replyAck
            + ", downstream DNs=" + Arrays.toString(downstreamDNs)
            + ", role=" + role
            + ", blockId=" + replicaInfo.getBlockId());
      } else if (LOG.isDebugEnabled()) {
        LOG.debug(myString + ", replyAck=" + replyAck);
      }

      // If a corruption was detected in the received data, terminate after
      // sending ERROR_CHECKSUM back.
      Status myStatus = PipelineAck.getStatusFromHeader(myHeader);
      if (myStatus == Status.ERROR_CHECKSUM) {
        throw new IOException("Shutting down writer and responder "
            + "due to a checksum error in received data. The error "
            + "response has been sent upstream.");
      }
    }

    /**
     * Remove a packet from the head of the ack queue
     *
     * This should be called only when the ack queue is not empty
     */
    protected void removeAckHead() {
      synchronized (ackQueue) {
        ackQueue.remove();
        ackQueue.notifyAll();
      }
    }
  }

  /**
   * This information is cached by the Datanode in the ackQueue.
   */
  protected static class Packet {
    final long seqno;
    final boolean lastPacketInBlock;
    final boolean lastPacketInStripe;
    final long offsetInBlock;
    final long ackEnqueueNanoTime;
    final Status ackStatus;

    Packet(long seqno, boolean lastPacketInBlock, boolean lastPacketInStripe,
           long offsetInBlock, long ackEnqueueNanoTime, Status ackStatus) {
      this.seqno = seqno;
      this.lastPacketInBlock = lastPacketInBlock;
      this.lastPacketInStripe = lastPacketInStripe;
      this.offsetInBlock = offsetInBlock;
      this.ackEnqueueNanoTime = ackEnqueueNanoTime;
      this.ackStatus = ackStatus;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(seqno=" + seqno
        + ", lastPacketInBlock=" + lastPacketInBlock
        + ", lastPacketInStripe=" + lastPacketInStripe
        + ", offsetInBlock=" + offsetInBlock
        + ", ackEnqueueNanoTime=" + ackEnqueueNanoTime
        + ", ackStatus=" + ackStatus
        + ")";
    }
  }
}
