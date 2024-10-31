///**
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package org.apache.hadoop.hdfs.server.datanode;
//
//import static org.apache.hadoop.hdfs.server.datanode.DataNode.DN_CLIENTTRACE_FORMAT;
//
//import java.io.BufferedOutputStream;
//import java.io.Closeable;
//import java.io.DataInputStream;
//import java.io.DataOutputStream;
//import java.io.EOFException;
//import java.io.IOException;
//import java.io.InterruptedIOException;
//import java.io.OutputStreamWriter;
//import java.io.Writer;
//import java.nio.ByteBuffer;
//import java.util.ArrayDeque;
//import java.util.Arrays;
//import java.util.Queue;
//import java.util.concurrent.atomic.AtomicLong;
//import java.util.zip.Checksum;
//
//import org.apache.commons.logging.Log;
//import org.apache.hadoop.fs.ChecksumException;
//import org.apache.hadoop.fs.FSOutputSummer;
//import org.apache.hadoop.fs.StorageType;
//import org.apache.hadoop.hdfs.DFSUtilClient;
//import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
//import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
//import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
//import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
//import org.apache.hadoop.hdfs.protocol.datatransfer.PacketReceiver;
//import org.apache.hadoop.hdfs.protocol.datatransfer.PipelineAck;
//import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
//import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
//import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaInputStreams;
//import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
//import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodePeerMetrics;
//import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
//import org.apache.hadoop.hdfs.util.DataTransferThrottler;
//import org.apache.hadoop.io.IOUtils;
//import org.apache.hadoop.util.Daemon;
//import org.apache.hadoop.util.DataChecksum;
//import org.apache.hadoop.util.StringUtils;
//import org.apache.hadoop.util.Time;
//import org.apache.htrace.core.Span;
//import org.apache.htrace.core.Tracer;
//
//import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.POSIX_FADV_DONTNEED;
//import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.SYNC_FILE_RANGE_WRITE;
//
//import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
//import org.slf4j.Logger;
//
///** A class that receives a block and writes to its own disk, meanwhile
// * may copies it to another site. If a throttler is provided,
// * streaming throttling is also supported.
// **/
//class BlockHybridReceiver extends BlockReceiver {
//
//    BlockHybridReceiver(final ExtendedBlock block, final StorageType storageType,
//      final DataInputStream in,
//      final String inAddr, final String myAddr,
//      final BlockConstructionStage stage,
//      final long newGs, final long minBytesRcvd, final long maxBytesRcvd,
//      final String clientname, final DatanodeInfo srcDataNode,
//      final DataNode datanode, DataChecksum requestedChecksum,
//      CachingStrategy cachingStrategy,
//      final boolean allowLazyPersist,
//      final boolean pinning,
//      final String storageId) throws IOException {
//        super(block, storageType, in, inAddr, myAddr, stage, newGs, minBytesRcvd, maxBytesRcvd,
//                clientname, srcDataNode, datanode, requestedChecksum, cachingStrategy,
//                allowLazyPersist, pinning, storageId);
//    }
//
//    protected int receivePacket() throws IOException {
//        // read the next packet
//        packetReceiver.receiveNextPacket(in);
//
//        PacketHeader header = packetReceiver.getHeader();
//        if (LOG.isDebugEnabled()){
//            LOG.debug("Receiving one packet for block " + block +
//                    ": " + header);
//        }
//        // offset might have been manually modified
//        long offsetInBlock;
//        if (header.getOffsetInBlock() > replicaInfo.getNumBytes()) {
//            offsetInBlock = replicaInfo.getNumBytes();
//        } else {
//            offsetInBlock = header.getOffsetInBlock();
//        }
//        // set mirror out depending on which offset you are writing to
//
//        if (header.getDataLen() < 0) {
//            throw new IOException("Got wrong length during writeBlock(" + block +
//                    ") from " + inAddr + " at offset " +
//                    header.getOffsetInBlock() + ": " +
//                    header.getDataLen());
//        }
//
//        long seqno = header.getSeqno();
//        boolean lastPacketInBlock = header.isLastPacketInBlock();
//        final int len = header.getDataLen();
//        boolean syncBlock = header.getSyncBlock();
//
//        // avoid double sync'ing on close
//        if (syncBlock && lastPacketInBlock) {
//            this.syncOnClose = false;
//            // sync directory for finalize irrespective of syncOnClose config since
//            // sync is requested.
//            this.dirSyncOnFinalize = true;
//        }
//
//        // update received bytes
//        final long firstByteInBlock = offsetInBlock;
//        offsetInBlock += len;
//        if (replicaInfo.getNumBytes() < offsetInBlock) {
//            replicaInfo.setNumBytes(offsetInBlock);
//        }
//
//        // put in queue for pending acks, unless sync was requested
//        if (responder != null && !syncBlock && !shouldVerifyChecksum()) {
//            ((PacketResponder) responder.getRunnable()).enqueue(seqno,
//                    lastPacketInBlock, offsetInBlock, Status.SUCCESS);
//        }
//
//        // Drop heartbeat for testing.
//        if (seqno < 0 && len == 0 &&
//                DataNodeFaultInjector.get().dropHeartbeatPacket()) {
//            return 0;
//        }
//
//        //First write the packet to the mirror:
//        if (mirrorOut != null && !mirrorError) {
//            try {
//                long begin = Time.monotonicNow();
//                // For testing. Normally no-op.
//                DataNodeFaultInjector.get().stopSendingPacketDownstream(mirrorAddr);
//                packetReceiver.mirrorPacketTo(mirrorOut);
//                mirrorOut.flush();
//                long now = Time.monotonicNow();
//                this.lastSentTime.set(now);
//                long duration = now - begin;
//                DataNodeFaultInjector.get().logDelaySendingPacketDownstream(
//                        mirrorAddr,
//                        duration);
//                trackSendPacketToLastNodeInPipeline(duration);
//                if (duration > datanodeSlowLogThresholdMs && LOG.isWarnEnabled()) {
//                    LOG.warn("Slow BlockReceiver write packet to mirror took " + duration
//                            + "ms (threshold=" + datanodeSlowLogThresholdMs + "ms), "
//                            + "downstream DNs=" + Arrays.toString(downstreamDNs)
//                            + ", blockId=" + replicaInfo.getBlockId());
//                }
//            } catch (IOException e) {
//                handleMirrorOutError(e);
//            }
//        }
//
//        ByteBuffer dataBuf = packetReceiver.getDataSlice();
//        ByteBuffer checksumBuf = packetReceiver.getChecksumSlice();
//
//        if (lastPacketInBlock || len == 0) {
//            if(LOG.isDebugEnabled()) {
//                LOG.debug("Receiving an empty packet or the end of the block " + block);
//            }
//            // sync block if requested
//            if (syncBlock) {
//                flushOrSync(true);
//            }
//        } else {
//            final int checksumLen = diskChecksum.getChecksumSize(len);
//            final int checksumReceivedLen = checksumBuf.capacity();
//
//            if (checksumReceivedLen > 0 && checksumReceivedLen != checksumLen) {
//                throw new IOException("Invalid checksum length: received length is "
//                        + checksumReceivedLen + " but expected length is " + checksumLen);
//            }
//
//            if (checksumReceivedLen > 0 && shouldVerifyChecksum()) {
//                try {
//                    verifyChunks(dataBuf, checksumBuf);
//                } catch (IOException ioe) {
//                    // checksum error detected locally. there is no reason to continue.
//                    if (responder != null) {
//                        try {
//                            ((PacketResponder) responder.getRunnable()).enqueue(seqno,
//                                    lastPacketInBlock, offsetInBlock,
//                                    Status.ERROR_CHECKSUM);
//                            // Wait until the responder sends back the response
//                            // and interrupt this thread.
//                            Thread.sleep(3000);
//                        } catch (InterruptedException e) { }
//                    }
//                    throw new IOException("Terminating due to a checksum error." + ioe);
//                }
//
//                if (needsChecksumTranslation) {
//                    // overwrite the checksums in the packet buffer with the
//                    // appropriate polynomial for the disk storage.
//                    translateChunks(dataBuf, checksumBuf);
//                }
//            }
//
//            if (checksumReceivedLen == 0 && !streams.isTransientStorage()) {
//                // checksum is missing, need to calculate it
//                checksumBuf = ByteBuffer.allocate(checksumLen);
//                diskChecksum.calculateChunkedSums(dataBuf, checksumBuf);
//            }
//
//            // by this point, the data in the buffer uses the disk checksum
//            final boolean shouldNotWriteChecksum = checksumReceivedLen == 0
//                    && streams.isTransientStorage();
//            try {
//                long onDiskLen = replicaInfo.getBytesOnDisk();
//                if (onDiskLen<offsetInBlock) {
//                    // Normally the beginning of an incoming packet is aligned with the
//                    // existing data on disk. If the beginning packet data offset is not
//                    // checksum chunk aligned, the end of packet will not go beyond the
//                    // next chunk boundary.
//                    // When a failure-recovery is involved, the client state and the
//                    // the datanode state may not exactly agree. I.e. the client may
//                    // resend part of data that is already on disk. Correct number of
//                    // bytes should be skipped when writing the data and checksum
//                    // buffers out to disk.
//                    long partialChunkSizeOnDisk = onDiskLen % bytesPerChecksum;
//                    long lastChunkBoundary = onDiskLen - partialChunkSizeOnDisk;
//                    boolean alignedOnDisk = partialChunkSizeOnDisk == 0;
//                    boolean alignedInPacket = firstByteInBlock % bytesPerChecksum == 0;
//
//                    // If the end of the on-disk data is not chunk-aligned, the last
//                    // checksum needs to be overwritten.
//                    boolean overwriteLastCrc = !alignedOnDisk && !shouldNotWriteChecksum;
//                    // If the starting offset of the packat data is at the last chunk
//                    // boundary of the data on disk, the partial checksum recalculation
//                    // can be skipped and the checksum supplied by the client can be used
//                    // instead. This reduces disk reads and cpu load.
//                    boolean doCrcRecalc = overwriteLastCrc &&
//                            (lastChunkBoundary != firstByteInBlock);
//
//                    // If this is a partial chunk, then verify that this is the only
//                    // chunk in the packet. If the starting offset is not chunk
//                    // aligned, the packet should terminate at or before the next
//                    // chunk boundary.
//                    if (!alignedInPacket && len > bytesPerChecksum) {
//                        throw new IOException("Unexpected packet data length for "
//                                +  block + " from " + inAddr + ": a partial chunk must be "
//                                + " sent in an individual packet (data length = " + len
//                                +  " > bytesPerChecksum = " + bytesPerChecksum + ")");
//                    }
//
//                    // If the last portion of the block file is not a full chunk,
//                    // then read in pre-existing partial data chunk and recalculate
//                    // the checksum so that the checksum calculation can continue
//                    // from the right state. If the client provided the checksum for
//                    // the whole chunk, this is not necessary.
//                    Checksum partialCrc = null;
//                    if (doCrcRecalc) {
//                        if (LOG.isDebugEnabled()) {
//                            LOG.debug("receivePacket for " + block
//                                    + ": previous write did not end at the chunk boundary."
//                                    + " onDiskLen=" + onDiskLen);
//                        }
//                        long offsetInChecksum = BlockMetadataHeader.getHeaderSize() +
//                                onDiskLen / bytesPerChecksum * checksumSize;
//                        partialCrc = computePartialChunkCrc(onDiskLen, offsetInChecksum);
//                    }
//
//                    // The data buffer position where write will begin. If the packet
//                    // data and on-disk data have no overlap, this will not be at the
//                    // beginning of the buffer.
//                    int startByteToDisk = (int)(onDiskLen-firstByteInBlock)
//                            + dataBuf.arrayOffset() + dataBuf.position();
//
//                    // Actual number of data bytes to write.
//                    int numBytesToDisk = (int)(offsetInBlock-onDiskLen);
//
//                    // Write data to disk.
//                    long begin = Time.monotonicNow();
//                    streams.writeDataToDisk(dataBuf.array(),
//                            startByteToDisk, numBytesToDisk);
//                    long duration = Time.monotonicNow() - begin;
//                    if (duration > datanodeSlowLogThresholdMs && LOG.isWarnEnabled()) {
//                        LOG.warn("Slow BlockReceiver write data to disk cost:" + duration
//                                + "ms (threshold=" + datanodeSlowLogThresholdMs + "ms), "
//                                + "volume=" + getVolumeBaseUri()
//                                + ", blockId=" + replicaInfo.getBlockId());
//                    }
//
//                    if (duration > maxWriteToDiskMs) {
//                        maxWriteToDiskMs = duration;
//                    }
//
//                    final byte[] lastCrc;
//                    if (shouldNotWriteChecksum) {
//                        lastCrc = null;
//                    } else {
//                        int skip = 0;
//                        byte[] crcBytes = null;
//
//                        // First, prepare to overwrite the partial crc at the end.
//                        if (overwriteLastCrc) { // not chunk-aligned on disk
//                            // prepare to overwrite last checksum
//                            adjustCrcFilePosition();
//                        }
//
//                        // The CRC was recalculated for the last partial chunk. Update the
//                        // CRC by reading the rest of the chunk, then write it out.
//                        if (doCrcRecalc) {
//                            // Calculate new crc for this chunk.
//                            int bytesToReadForRecalc =
//                                    (int)(bytesPerChecksum - partialChunkSizeOnDisk);
//                            if (numBytesToDisk < bytesToReadForRecalc) {
//                                bytesToReadForRecalc = numBytesToDisk;
//                            }
//
//                            partialCrc.update(dataBuf.array(), startByteToDisk,
//                                    bytesToReadForRecalc);
//                            byte[] buf = FSOutputSummer.convertToByteStream(partialCrc,
//                                    checksumSize);
//                            crcBytes = copyLastChunkChecksum(buf, checksumSize, buf.length);
//                            checksumOut.write(buf);
//                            if(LOG.isDebugEnabled()) {
//                                LOG.debug("Writing out partial crc for data len " + len +
//                                        ", skip=" + skip);
//                            }
//                            skip++; //  For the partial chunk that was just read.
//                        }
//
//                        // Determine how many checksums need to be skipped up to the last
//                        // boundary. The checksum after the boundary was already counted
//                        // above. Only count the number of checksums skipped up to the
//                        // boundary here.
//                        long skippedDataBytes = lastChunkBoundary - firstByteInBlock;
//
//                        if (skippedDataBytes > 0) {
//                            skip += (int)(skippedDataBytes / bytesPerChecksum) +
//                                    ((skippedDataBytes % bytesPerChecksum == 0) ? 0 : 1);
//                        }
//                        skip *= checksumSize; // Convert to number of bytes
//
//                        // write the rest of checksum
//                        final int offset = checksumBuf.arrayOffset() +
//                                checksumBuf.position() + skip;
//                        final int end = offset + checksumLen - skip;
//                        // If offset >= end, there is no more checksum to write.
//                        // I.e. a partial chunk checksum rewrite happened and there is no
//                        // more to write after that.
//                        if (offset >= end && doCrcRecalc) {
//                            lastCrc = crcBytes;
//                        } else {
//                            final int remainingBytes = checksumLen - skip;
//                            lastCrc = copyLastChunkChecksum(checksumBuf.array(),
//                                    checksumSize, end);
//                            checksumOut.write(checksumBuf.array(), offset, remainingBytes);
//                        }
//                    }
//
//                    /// flush entire packet, sync if requested
//                    flushOrSync(syncBlock);
//
//                    replicaInfo.setLastChecksumAndDataLen(offsetInBlock, lastCrc);
//
//                    datanode.metrics.incrBytesWritten(len);
//                    datanode.metrics.incrTotalWriteTime(duration);
//
//                    manageWriterOsCache(offsetInBlock);
//                }
//            } catch (IOException iex) {
//                // Volume error check moved to FileIoProvider
//                throw iex;
//            }
//        }
//
//        // if sync was requested, put in queue for pending acks here
//        // (after the fsync finished)
//        if (responder != null && (syncBlock || shouldVerifyChecksum())) {
//            ((PacketResponder) responder.getRunnable()).enqueue(seqno,
//                    lastPacketInBlock, offsetInBlock, Status.SUCCESS);
//        }
//
//        /*
//         * Send in-progress responses for the replaceBlock() calls back to caller to
//         * avoid timeouts due to balancer throttling. HDFS-6247
//         */
//        if (isReplaceBlock
//                && (Time.monotonicNow() - lastResponseTime > responseInterval)) {
//            BlockOpResponseProto.Builder response = BlockOpResponseProto.newBuilder()
//                    .setStatus(Status.IN_PROGRESS);
//            response.build().writeDelimitedTo(replyOut);
//            replyOut.flush();
//
//            lastResponseTime = Time.monotonicNow();
//        }
//
//        if (throttler != null) { // throttle I/O
//            throttler.throttle(len);
//        }
//
//        return lastPacketInBlock?-1:len;
//    }
//}
