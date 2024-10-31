package org.apache.hadoop.hdfs.server.datanode.erasurecode.transcoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.util.DataChecksum;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;

public class StripedWriter {

  private static final Logger LOG = DataNode.LOG;
  private final static int WRITE_PACKET_SIZE = 64 * 1024;

  private final StripedTranscoder transcoder;
  private final DataNode datanode;
  private final Configuration conf;

  // target info
  final ExtendedBlock targetBlock;
  final DatanodeInfo[] targets;
  final StorageType[] targetStorageTypes;
  final String[] targetStorageIds;

  // writing configs/objs
  private final StripedBlockWriter[] writers;

  private int maxChunksPerPacket;
  private byte[] packetBuf;
  private byte[] checksumBuf;
  private int bytesPerChecksum;
  private int checksumSize;

  StripedWriter(StripedTranscoder transcoder, DataNode datanode,
                Configuration conf, StripedTranscodingInfo transcoderInfo) {
    this.transcoder = transcoder;
    this.datanode = datanode;
    this.conf = conf;

    this.targetBlock = transcoderInfo.getTargetBlock();
    this.targets = transcoderInfo.getTargets();
    this.targetStorageTypes = transcoderInfo.getTargetStorageTypes();
    this.targetStorageIds = transcoderInfo.getTargetStorageIds();

    this.writers = new StripedBlockWriter[targets.length];
  }

  void init() throws IOException {
    initPackets();

    initWriters();
  }

  void initPackets() {
    DataChecksum checksum = transcoder.reader.getChecksum();
    checksumSize = checksum.getChecksumSize();
    bytesPerChecksum = checksum.getBytesPerChecksum();
    int chunkSize = bytesPerChecksum + checksumSize;
    maxChunksPerPacket = Math.max(
        (WRITE_PACKET_SIZE - PacketHeader.PKT_MAX_HEADER_LEN) / chunkSize, 1);
    int maxPacketSize = chunkSize * maxChunksPerPacket
        + PacketHeader.PKT_MAX_HEADER_LEN;

    packetBuf = new byte[maxPacketSize];
    int tmpLen = checksumSize *
        (transcoder.reader.getBufferSize() / bytesPerChecksum);
    checksumBuf = new byte[tmpLen];
  }

  private void initWriters() throws IOException {
    for (short i = 0; i < targets.length; i++) {
      try {
        writers[i] = createWriter(i);
      } catch (Throwable e) {
        throw new IOException("Unable to make connections to all targets");
      }
    }
  }

  private StripedBlockWriter createWriter(short index) throws IOException {
    ExtendedBlock b = new ExtendedBlock(targetBlock);
    b.setBlockId(targetBlock.getBlockId() + index);
    return new StripedBlockWriter(this, datanode, conf, b, targets[index],
        targetStorageTypes[index], targetStorageIds[index], transcoder.getCachingStrategy());
  }

  ByteBuffer[] getTargetBuffers(int toTranscodeLen) {
    ByteBuffer[] outputs = new ByteBuffer[targets.length];
    for (int i = 0; i < targets.length; i++) {
      writers[i].getTargetBuffer().limit(toTranscodeLen);
      outputs[i] = writers[i].getTargetBuffer();
    }
    return outputs;
  }

  void updateTargetBuffers(int toTranscodeLen) {
    for (int i = 0; i < targets.length; i++) {
      long blockLen = transcoder.reader.getTargetLen();
      long remaining = blockLen - transcoder.positionInBlock;
      if (remaining <= 0) {
        writers[i].getTargetBuffer().limit(0);
      } else if (remaining < toTranscodeLen) {
        writers[i].getTargetBuffer().limit((int) remaining);
      }
    }
  }

  /**
   * Send newly transcoded parities to target destinations.
   */
  void transferData2Targets() throws IOException {
    for (int i = 0; i < targets.length; i++) {
      try {
        writers[i].transferData2Target(packetBuf);
      } catch (Throwable e) {
        throw new IOException("Unable to transfer data to all targets");
      }
    }
  }

  /**
   * Send an empty packet to mark the end of the block.
   */
  void endTargetBlocks() {
    for (int i = 0; i < targets.length; i++) {
      try {
        writers[i].endTargetBlock(packetBuf);
      } catch (IOException e) {
        LOG.warn(e.getMessage());
      }
    }
  }

  void clearBuffers() {
    for (StripedBlockWriter writer : writers) {
      if (writer.getTargetBuffer() != null) {
        writer.getTargetBuffer().clear();
      }
    }
  }

  void close() {
    for (StripedBlockWriter writer : writers) {
      ByteBuffer targetBuffer =
          writer != null ? writer.getTargetBuffer() : null;
      if (targetBuffer != null) {
        transcoder.releaseBuffer(targetBuffer);
        writer.freeTargetBuffer();
      }
    }

    for (int i = 0; i < targets.length; i++) {
      if (writers[i] != null) {
        writers[i].close();
      }
    }
  }

  // bytebuffer pool methods
  protected ByteBuffer allocateWriteBuffer() {
    return transcoder.allocateBuffer(transcoder.reader.getBufferSize());
  }

  protected ByteBuffer allocateChecksumBuffer() {
    return transcoder.allocateBuffer(true, checksumBuf.length);
  }

  protected void releaseBuffer(ByteBuffer buffer) {
    transcoder.releaseBuffer(buffer);
  }

  // used by striped block writer to access metadata
  byte[] getChecksumBuf() {
    return checksumBuf;
  }

  int getBytesPerChecksum() {
    return bytesPerChecksum;
  }

  int getChecksumSize() {
    return checksumSize;
  }

  DataChecksum getChecksum() {
    return transcoder.reader.getChecksum();
  }

  int getMaxChunksPerPacket() {
    return maxChunksPerPacket;
  }
}
