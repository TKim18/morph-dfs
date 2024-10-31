package org.apache.hadoop.hdfs.server.datanode.erasurecode.transcoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSPacket;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataEncryptionKeyFactory;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.EnumSet;

public class StripedBlockWriter {

  private final StripedWriter writer;
  private final DataNode datanode;
  private final Configuration conf;

  private final ExtendedBlock block;
  private final DatanodeInfo target;
  private final StorageType storageType;
  private final String storageId;

  private final CachingStrategy cachingStrategy;
  private Socket targetSocket;
  private DataOutputStream targetOutputStream;
  private DataInputStream targetInputStream;
  private ByteBuffer targetBuffer;
  private long blockOffset4Target = 0;
  private long seqNo4Target = 0;

  StripedBlockWriter(StripedWriter writer, DataNode datanode,
                     Configuration conf, ExtendedBlock block,
                     DatanodeInfo target, StorageType storageType,
                     String storageId, CachingStrategy cachingStrategy) throws IOException {
    this.writer = writer;
    this.datanode = datanode;
    this.conf = conf;

    this.block = block;
    this.target = target;
    this.storageType = storageType;
    this.storageId = storageId;

    this.cachingStrategy = cachingStrategy;
    this.targetBuffer = writer.allocateWriteBuffer();

    init();
  }

  ByteBuffer getTargetBuffer() {
    return targetBuffer;
  }

  void freeTargetBuffer() {
    targetBuffer = null;
  }

  InetSocketAddress getSocketAddress4Transfer(DatanodeInfo dnInfo) {
    return NetUtils.createSocketAddr(dnInfo.getXferAddr(
        datanode.getDnConf().getConnectToDnViaHostname()));
  }

  private void init() throws IOException {
    Socket socket = null;
    DataOutputStream out = null;
    DataInputStream in = null;
    boolean success = false;
    try {
      InetSocketAddress targetAddr = getSocketAddress4Transfer(target);
      socket = datanode.newSocket();
      NetUtils.connect(socket, targetAddr,
          datanode.getDnConf().getSocketTimeout());
      socket.setTcpNoDelay(
          datanode.getDnConf().getDataTransferServerTcpNoDelay());
      socket.setSoTimeout(datanode.getDnConf().getSocketTimeout());

      Token<BlockTokenIdentifier> blockToken =
          datanode.getBlockAccessToken(block,
              EnumSet.of(BlockTokenIdentifier.AccessMode.WRITE),
              new StorageType[]{storageType}, new String[]{storageId});

      long writeTimeout = datanode.getDnConf().getSocketWriteTimeout();
      OutputStream unbufOut = NetUtils.getOutputStream(socket, writeTimeout);
      InputStream unbufIn = NetUtils.getInputStream(socket);
      DataEncryptionKeyFactory keyFactory =
          datanode.getDataEncryptionKeyFactoryForBlock(block);
      IOStreamPair saslStreams = datanode.getSaslClient().socketSend(
          socket, unbufOut, unbufIn, keyFactory, blockToken, target);

      unbufOut = saslStreams.out;
      unbufIn = saslStreams.in;

      out = new DataOutputStream(new BufferedOutputStream(unbufOut,
          DFSUtilClient.getSmallBufferSize(conf)));
      in = new DataInputStream(unbufIn);

      DatanodeInfo source = new DatanodeInfo.DatanodeInfoBuilder()
          .setNodeID(datanode.getDatanodeId()).build();
      new Sender(out).writeBlock(block, storageType,
          blockToken, "", new DatanodeInfo[]{target},
          new StorageType[]{storageType}, source,
          BlockConstructionStage.PIPELINE_SETUP_CREATE, 0, 0, 0, 0,
          writer.getChecksum(), cachingStrategy,
          false, false, null, storageId, new String[]{storageId});

      targetSocket = socket;
      targetOutputStream = out;
      targetInputStream = in;
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeStream(out);
        IOUtils.closeStream(in);
        IOUtils.closeStream(socket);
      }
    }
  }

  /**
   * Send data to targets.
   */
  void transferData2Target(byte[] packetBuf) throws IOException {
    if (targetBuffer.remaining() == 0) {
      return;
    }

    if (targetBuffer.isDirect()) {
      ByteBuffer directCheckSumBuf = writer.allocateChecksumBuffer();
      writer.getChecksum().calculateChunkedSums(
          targetBuffer, directCheckSumBuf);
      directCheckSumBuf.get(writer.getChecksumBuf());
      writer.releaseBuffer(directCheckSumBuf);
    } else {
      writer.getChecksum().calculateChunkedSums(
          targetBuffer.array(), 0, targetBuffer.remaining(),
          writer.getChecksumBuf(), 0);
    }

    int ckOff = 0;
    while (targetBuffer.remaining() > 0) {
      DFSPacket packet = new DFSPacket(packetBuf,
          writer.getMaxChunksPerPacket(),
          blockOffset4Target, seqNo4Target++,
          writer.getChecksumSize(), false, false);
      int maxBytesToPacket = writer.getMaxChunksPerPacket()
          * writer.getBytesPerChecksum();
      int toWrite = Math.min(targetBuffer.remaining(), maxBytesToPacket);
      int ckLen = ((toWrite - 1) / writer.getBytesPerChecksum() + 1)
          * writer.getChecksumSize();
      packet.writeChecksum(writer.getChecksumBuf(), ckOff, ckLen);
      ckOff += ckLen;
      packet.writeData(targetBuffer, toWrite);

      // Send packet
      packet.writeTo(targetOutputStream);

      blockOffset4Target += toWrite;
    }
  }

  // send an empty packet to mark the end of the block
  void endTargetBlock(byte[] packetBuf) throws IOException {
    DFSPacket packet = new DFSPacket(packetBuf, 0,
        blockOffset4Target, seqNo4Target++,
        writer.getChecksumSize(), true, true);
    packet.writeTo(targetOutputStream);
    targetOutputStream.flush();
  }

  void close() {
    IOUtils.closeStream(targetOutputStream);
    IOUtils.closeStream(targetInputStream);
    IOUtils.closeStream(targetSocket);
  }
}
