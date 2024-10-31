package org.apache.hadoop.hdfs.server.datanode.erasurecode.transcoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.ReplicaHandler;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInPipeline;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.util.AutoCloseableLock;
import org.apache.hadoop.util.DataChecksum;
import org.slf4j.Logger;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class StripedMerger {

  private static final Logger LOG = DataNode.LOG;

  private final StripedTranscoder transcoder;
  private final DataNode datanode;
  private final Configuration conf;

  final ExtendedBlock[] sourceBlocks;
  final DatanodeInfo[] sources;
  final int[] blockLengths;
  private final int numParities;
  int parityIndex;
  FsDatasetSpi<?> data;

  StripedMerger(StripedTranscoder transcoder, DataNode datanode,
                Configuration conf, StripedTranscodingInfo info) {
    this.transcoder = transcoder;
    this.datanode = datanode;
    this.conf = conf;

    this.numParities = info.getEcPolicy().getNumParityUnits();
    // parse source block and id offsets based on list of parity blocks and ec scheme
    this.sourceBlocks = info.getParityBlocks();
    // sources should only be the relevant parity sources for this node
    this.sources = info.getParitySources();

    int numParityBlocks = info.getParityBlocks().length;
    int numParityChunks = info.getParitySources().length;
    int i = 0;
    for (; i < numParityChunks; i++) {
      if (info.getParitySources()[i].equals(info.getTargets()[0])) {
        // found the index for which we are merging
        this.parityIndex = i;
        break;
      }
    }
    if (i == numParityChunks) {
      LOG.error("Parity chunks are not co-located - can't perform local merge");
    }

    // fill out the parity length for each parity block
    this.blockLengths = new int[numParityBlocks];
    for (int k = 0; k < numParityBlocks; k++) {
      this.blockLengths[k] = (int) (this.sourceBlocks[k].getNumBytes() / numParities);
    }
  }

  void init() {
    this.data = datanode.getFSDataset();
  }

  ByteBuffer readAndCombineChunks() throws IOException {
    int bufferLength = blockLengths[0];
    ByteBuffer buffer = transcoder.allocateBuffer(bufferLength);

    try {
      // initialize buffer for final result and tmp for temporary reading
      byte[] tmp = new byte[bufferLength];

      // read the local chunks
      for (int i = 0; i < sourceBlocks.length; i++) {
        // for each parity block, read the local block into the buffer
        ExtendedBlock localBlock = new ExtendedBlock(sourceBlocks[i]);
        localBlock.setBlockId(localBlock.getBlockId() + this.parityIndex);

        int n;
        try (InputStream file = data.getBlockInputStream(localBlock, 0)) {
          if (i == 0) {
            // directly read into buffer
            n = file.read(buffer.array());
          } else {
            // read into temporary buffer
            n = file.read(tmp);
            for (int b = 0; b < bufferLength; b++) {
              // re-insert into buffer the xor of parities
              buffer.put(b, (byte) (buffer.get(b) ^ tmp[b]));
            }
          }
          if (n != bufferLength) {
            LOG.warn("Unable to read entire block when merging");
          }
        }
      }
      return buffer;
    } catch (Exception e) {
      LOG.error("Exception while doing " +
          "short-circuited local read and merge", e);
      throw e;
    }
  }

  DataChecksum getLocalChecksum() {
    // TODO: can be a lot better about this, but for now hard-code the checksum
    return DataChecksum.newDataChecksum(DataChecksum.Type.CRC32C, 512);
  }

  void writeToLocalDisk(ByteBuffer buffer, DataChecksum checksum) throws IOException {
    int bufferLength = buffer.limit();
    int checksumLength = checksum.getChecksumSize(bufferLength);
    ByteBuffer checksumBuffer = transcoder.allocateBuffer(checksumLength);
    checksum.calculateChunkedSums(buffer, checksumBuffer);

    ExtendedBlock targetBlock = new ExtendedBlock(transcoder.writer.targetBlock);
    targetBlock.setBlockId(targetBlock.getBlockId() + this.parityIndex);
    targetBlock.setNumBytes(bufferLength);

    try (ReplicaHandler replicaHandler = data.createRbw(
        transcoder.writer.targetStorageTypes[0],
        transcoder.writer.targetStorageIds[0],
        targetBlock, false, false)) {
      ReplicaInPipeline replica = replicaHandler.getReplica();
      replica.setNumBytes(bufferLength);

      ReplicaOutputStreams streams = replica.createStreams(true, checksum);
      DataOutputStream checksumOut = new DataOutputStream(new BufferedOutputStream(
          streams.getChecksumOut(), DFSUtilClient.getSmallBufferSize(conf)));

      // write checksum and data
      BlockMetadataHeader.writeHeader(checksumOut, checksum);
      streams.writeDataToDisk(buffer.array(), 0, buffer.limit());

      checksumOut.write(checksumBuffer.array(), 0, checksumLength);
      final byte[] lastCrc = Arrays.copyOfRange(
          checksumBuffer.array(), checksumLength - checksum.getChecksumSize(), checksumLength);

      // flush data and checksum
      checksumOut.flush();
      streams.flushDataOut();
      streams.syncDataOut();
      replica.setLastChecksumAndDataLen(bufferLength, lastCrc);
      replica.setBytesAcked(bufferLength);

      // finalize block and notify namenode
      data.finalizeBlock(targetBlock, false);
      datanode.closeBlock(targetBlock, null,
          replica.getStorageUuid(), replica.isOnTransientStorage());

      // close out resources
      streams.closeDataStream();
      streams.close();
      checksumOut.close();
    } catch (Exception e) {
      LOG.error("Exception while writing new replica during local merge", e);
      throw e;
    } finally {
      // release buffers
      transcoder.releaseBuffer(buffer);
      transcoder.releaseBuffer(checksumBuffer);
    }
  }

  /**
   * This is a short-circuited local merge because parities are co-located.
   */
  void merge() {
    final long startTime = System.currentTimeMillis();
    long write = System.currentTimeMillis();
    try {
      // read and xor parity chunks into a single bytebuffer
      ByteBuffer chunk = readAndCombineChunks();

      // determine checksum from local file
      write = System.currentTimeMillis();
      DataChecksum checksum = getLocalChecksum();

      // write new chunk to local disk
      writeToLocalDisk(chunk, checksum);
    } catch (Exception e) {
      LOG.error("Exception while transcoding local parities for block {} at index {}",
          transcoder.writer.targetBlock.getBlockId(), parityIndex, e);
    } finally {
      LOG.info("Completed local parity merge in {} ms for block {} at index {}. Reading took {} and writing took {}.",
          (System.currentTimeMillis() - startTime),
          transcoder.writer.targetBlock.getBlockId(), parityIndex,
          (write - startTime), (System.currentTimeMillis() - write));
    }
  }
}
