package org.apache.hadoop.hdfs.server.datanode;

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
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.EnumSet;

public class ParityWriter {
  private final ParityHandler parityHandler;
  private final DataNode datanode;
  private final Configuration conf;

  private final ExtendedBlock block;
  private final DatanodeInfo target;
  private final StorageType storageType;
  private final String storageId;

  private Socket targetSocket;
  private DataOutputStream targetOutputStream;
  private DataInputStream targetInputStream;
  private ByteBuffer targetBuffer;
  private long blockOffset4Target = 0;
  private long seqNo4Target = 0;
  private static final ByteBufferPool BUFFER_POOL = new ElasticByteBufferPool();

  ParityWriter(ParityHandler parityHandler, DataNode datanode,
               Configuration conf, ExtendedBlock block,
               DatanodeInfo target, StorageType storageType,
               String storageId, int cellSize) throws IOException {
    this.parityHandler = parityHandler;
    this.datanode = datanode;
    this.conf = conf;

    this.block = block;
    this.target = target;
    this.storageType = storageType;
    this.storageId = storageId;

    this.targetBuffer = parityHandler.allocateBuffer(cellSize);

    init();
  }

  ByteBuffer getTargetBuffer() {
    return targetBuffer;
  }

  void freeTargetBuffer() {
    targetBuffer = null;
  }

  /**
   * Initialize  output/input streams for transferring data to target
   * and send create block request.
   */
  private void init() throws IOException {
    Socket socket = null;
    DataOutputStream out = null;
    DataInputStream in = null;
    boolean success = false;
    try {
      InetSocketAddress targetAddr = NetUtils.createSocketAddr(target.getXferAddr(
          datanode.getDnConf().getConnectToDnViaHostname()));
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
          parityHandler.getChecksum(), CachingStrategy.newDefaultStrategy(),
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

    DataChecksum sum = parityHandler.getChecksum();
    if (targetBuffer.isDirect()) {
      ByteBuffer directCheckSumBuf =
          BUFFER_POOL.getBuffer(true, parityHandler.getChecksumBuf().length);
      sum.calculateChunkedSums(targetBuffer, directCheckSumBuf);
      directCheckSumBuf.get(parityHandler.getChecksumBuf());
      BUFFER_POOL.putBuffer(directCheckSumBuf);
    } else {
      sum.calculateChunkedSums(targetBuffer.array(), 0,
          targetBuffer.remaining(), parityHandler.getChecksumBuf(), 0);
    }

    int ckOff = 0;
    while (targetBuffer.remaining() > 0) {
      DFSPacket packet = new DFSPacket(packetBuf,
          parityHandler.getMaxChunksPerPacket(),
          blockOffset4Target, seqNo4Target++,
          parityHandler.getChecksumSize(), false, false);
      int maxBytesToPacket = parityHandler.getMaxChunksPerPacket()
          * parityHandler.getBytesPerChecksum();
      int toWrite = Math.min(targetBuffer.remaining(), maxBytesToPacket);
      int ckLen = ((toWrite - 1) / parityHandler.getBytesPerChecksum() + 1)
          * parityHandler.getChecksumSize();
      packet.writeChecksum(parityHandler.getChecksumBuf(), ckOff, ckLen);
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
        parityHandler.getChecksumSize(), true, true);
    packet.writeTo(targetOutputStream);
    targetOutputStream.flush();
  }

  void close() {
    IOUtils.closeStream(targetOutputStream);
    IOUtils.closeStream(targetInputStream);
    IOUtils.closeStream(targetSocket);
  }
}
