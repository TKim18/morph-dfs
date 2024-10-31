package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.util.DataChecksum;
import org.slf4j.Logger;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * ParityHandler will receive data from dataxceiver instances,
 * compute new parities based on the ec policy provided and buffer sizes,
 * and send out the parities to their destined locations.
 *
 * This is any asynchronous path that does not impact client latency.
 * This class implies that the data of the same block group makes it
 * to the same datanode.
 *
 * We spin up a new instance of parity sender each time a new set of parities
 * needs to be computed and send out.
 */
public class ParityHandler implements Runnable {

  private static final int CHECKSUM_SIZE = 512;
  private final static int PACKET_SIZE = 64 * 1024;


  public static final Logger LOG = DataNode.LOG;
  private static final ByteBufferPool BUFFER_POOL = new ElasticByteBufferPool();

  private final ExtendedBlock block;
  private final DataNode datanode;
  private final Configuration conf;

  // Fields below are for storing data and computing parities
  private final BlockBuffer blockBuffer; // map stripe/row index to its cellBuffer
  private final RawErasureEncoder encoder;
  private final DataChecksum sum;
  private final int numDataUnits;
  private final int numParityUnits;
  private final int cellSize;
  private final int blockSize;

  // Fields below are for writing final parities
  private final ParityWriter[] writers;
  private final DatanodeInfo[] parityTargets;
  private final StorageType[] parityStorageTypes;
  private final String[] parityStorageIds;

  private int maxChunksPerPacket;
  private byte[] packetBuf;
  private byte[] checksumBuf;
  private int bytesPerChecksum;
  private int checksumSize;
  // Fields are for management
  private boolean terminated;
  private boolean dataReceived;

  /**
   * Internal class to potentially hold all data from block group divided
   * into cellBuffers, each holding a stripe's worth of data.
   */
  class BlockBuffer {
    private final CellBuffers[] stripeBuffers;
    private final int numStripelets;
    private int row;  // which stripelet
    private int column; // which cell in the stripelet

    // a lock on every cell buffer, initially fully locked
    // the cellbuffer is only unlocked when all the data for that buffer is received
    private final Semaphore[] locks;

    BlockBuffer() throws InterruptedException {
      numStripelets = blockSize / cellSize;
      this.stripeBuffers = new CellBuffers[numStripelets];
      this.locks = new Semaphore[numStripelets];
      for (int i = 0; i < numStripelets; i++) {
        this.stripeBuffers[i] = new CellBuffers();
        this.locks[i] = new Semaphore(1);
        this.locks[i].acquire();
      }
      row = 0;
      column = 0;
    }

    /**
     * Adds to the corresponding CellBuffer or stripelet object
     * Row = which stripelet, column = which cell in the stripelet
     * This method assumes the current CellBuffer is locked and only
     * unlocks the row when all data is received for that row.
     * @param b
     * @param offset
     * @param length
     */
    private void addTo(byte[] b, int offset, int length) {
      final int pos = stripeBuffers[row].addTo(column, b, offset, length);
      if (pos == cellSize) {
        // filled a cell, move index down
        if (++column == numDataUnits) {
          // at the end of the cellBuffer, unlock it so background thread can process
          locks[row].release();
          row++;
          column = 0;
        }
      }
    }

    void unlock() {
      // unlock all locks
      for (int i = 0; i < numStripelets; i++) {
        locks[i].release();
      }
    }

    void release() {
      for (int i = 0; i < numStripelets; i++) {
        stripeBuffers[i].release();
      }
    }
  }

  class CellBuffers {
    private final ByteBuffer[] buffers;

    CellBuffers() {
      buffers = new ByteBuffer[numDataUnits];
      for (int i = 0; i < buffers.length; i++) {
        buffers[i] = BUFFER_POOL.getBuffer(useDirectBuffer(), cellSize);
        buffers[i].limit(cellSize);
      }
    }

    private int addTo(int i, byte[] b, int offset, int len) {
      final ByteBuffer buf = buffers[i];
      final int pos = buf.position() + len;
      Preconditions.checkState(pos <= cellSize);
      buf.put(b, offset, len);
      return pos;
    }
    private void clear() {
      for (int i = 0; i < buffers.length; i++) {
        buffers[i].clear();
        buffers[i].limit(cellSize);
      }
    }

    private void release() {
      for (int i = 0; i < buffers.length; i++) {
        if (buffers[i] != null) {
          BUFFER_POOL.putBuffer(buffers[i]);
          buffers[i] = null;
        }
      }
    }

    private void flipDataBuffers() {
      for (int i = 0; i < numDataUnits; i++) {
        buffers[i].flip();
      }
    }
  }

  ParityHandler(DataNode datanode, ExtendedBlock block,
                ErasureCodingPolicy ecPolicy, int blockSize,
                DatanodeInfo[] targets, StorageType[] storageTypes, String[] storageIds) throws Exception {
    this.datanode = datanode;
    this.conf = datanode.getConf();
    this.block = block;
    this.numDataUnits = ecPolicy.getNumDataUnits();
    this.numParityUnits = ecPolicy.getNumParityUnits();
    this.cellSize = ecPolicy.getCellSize();
    this.blockSize = blockSize;

    // writers sends out parities
    this.writers = new ParityWriter[numParityUnits];
    this.parityTargets = targets;
    this.parityStorageTypes = storageTypes;
    this.parityStorageIds = storageIds;

    // track internal state
    this.terminated = false;

    // data structures that need to be initialized before running
    ErasureCoderOptions coderOptions = new ErasureCoderOptions(numDataUnits, numParityUnits);
    this.encoder = CodecUtil.createRawEncoder(conf, ecPolicy.getCodecName(), coderOptions);
    this.sum = DataChecksum.newDataChecksum(DataChecksum.Type.CRC32C, CHECKSUM_SIZE);
    this.blockBuffer = new BlockBuffer(); // initialize a list of cellBuffers
  }

  /**
   * Receives data from a DataXceiver instance which is used
   * to compute parities and sent to final nodes at the end.
   */
  @Override
  public void run() {
    init();
    final long startTime = System.currentTimeMillis();
    try {
      // for each stripe in the block group
      LOG.info("Starting a run for block {}", block.getBlockId());
      for (int i = 0; i < blockBuffer.numStripelets; i++) {
        // wait until the cellBuffer is complete
        blockBuffer.locks[i].acquire();

        // it's possible you acquired because you have no more data
        // in which case hybrid doesn't write parities for incomplete stripes
        // so just skip to end blocks
        if (terminated) {
          break;
        }

        // prep buffers
        blockBuffer.stripeBuffers[i].flipDataBuffers();

        // encode parities
        calculateParity(blockBuffer.stripeBuffers[i].buffers);

        // send parities out to destinations
        sendParity();

        // clear and reset the buffers for next iteration
        clearBuffers(blockBuffer.stripeBuffers[i]);
      }
      // notify blocks that we are done
      endBlocks();
    } catch (InterruptedException e) {
      // possibly interrupted because no more data is coming
      LOG.info("Parity handler was interrupted");
    } catch (Exception e) {
      LOG.error("Exception while async handling parities on server-side:", e);
    } finally {
      // remove tracking from xserver on datanode
      LOG.info("Ending a run for block {} in {} ms",
          block.getBlockId(), System.currentTimeMillis() - startTime);
      for (ParityWriter writer : writers) {
        if (writer != null) {
          freeBuffer(writer.getTargetBuffer());
          writer.freeTargetBuffer();
          writer.close();
        }
      }
      blockBuffer.release();
      datanode.xserver.closeParityHandler(block);
    }
  }

  private void init() {
    // initialize parity writers here
    for (int i = 0; i < numParityUnits; i++) {
      try {
        ExtendedBlock parityBlock = new ExtendedBlock(block);
        parityBlock.setBlockId(parityBlock.getBlockId() + i);
        writers[i] = new ParityWriter(this, datanode, conf, parityBlock,
            parityTargets[i], parityStorageTypes[i], parityStorageIds[i], cellSize);
      } catch (Throwable e) {
        LOG.error(e.getMessage());
      }
    }

    checksumSize = sum.getChecksumSize();
    bytesPerChecksum = sum.getBytesPerChecksum();
    int chunkSize = bytesPerChecksum + checksumSize;
    maxChunksPerPacket = Math.max(
        (PACKET_SIZE - PacketHeader.PKT_MAX_HEADER_LEN) / chunkSize, 1);
    int maxPacketSize = chunkSize * maxChunksPerPacket
        + PacketHeader.PKT_MAX_HEADER_LEN;
    packetBuf = new byte[maxPacketSize];
    int tmpLen = checksumSize * (cellSize / bytesPerChecksum);
    checksumBuf = new byte[tmpLen];
  }

  /**
   * Dump buffer takes and writes data into CellBuffer. This method
   * should only be called by a different thread than run().
   */
  public void writeToParityHandler(ByteBuffer dataBuf) throws IOException {
    // write raw data into buffer
    try {
      blockBuffer.addTo(dataBuf.array(), dataBuf.arrayOffset(), dataBuf.remaining());
    } catch (Exception e) {
      LOG.error("Unable to receive packet buffer in parity handler", e);
    }
  }

  public void releaseLocks() {
    // this unlocks all the row buffers
    terminated = true;
    blockBuffer.unlock();
  }

  ByteBuffer[] getTargetBuffers() {
    ByteBuffer[] outputs = new ByteBuffer[numParityUnits];
    for (int i = 0; i < numParityUnits; i++) {
      writers[i].getTargetBuffer().limit(cellSize);
      outputs[i] = writers[i].getTargetBuffer();
    }
    return outputs;
  }

  private void clearBuffers(CellBuffers cellBuffers) {
    cellBuffers.clear();
    for (int i = 0; i < numParityUnits; i++) {
      writers[i].getTargetBuffer().clear();
    }
  }

  public void calculateParity(ByteBuffer[] dataBuffers) throws IOException {
    // use data in cellBuffer to compute parities
    ByteBuffer[] parityBuffers = getTargetBuffers();
    encoder.encode(dataBuffers, parityBuffers);
  }

  private void sendParity() {
    // send off parity cells to destinations
    for (int i = 0; i < numParityUnits; i++) {
      try {
        writers[i].transferData2Target(packetBuf);
      } catch (IOException e) {
        LOG.error("Error while async sending parities");
      }
    }
  }

  private void endBlocks() {
    for (int i = 0; i < numParityUnits; i++) {
      try {
        writers[i].endTargetBlock(packetBuf);
      } catch (IOException e) {
        LOG.error(e.getMessage());
      }
    }
  }

  private boolean useDirectBuffer() {
    return encoder.preferDirectBuffer();
  }

  ByteBuffer allocateBuffer(int length) {
    return BUFFER_POOL.getBuffer(useDirectBuffer(), length);
  }

  void freeBuffer(ByteBuffer buffer) {
    BUFFER_POOL.putBuffer(buffer);
  }

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
    return sum;
  }

  int getMaxChunksPerPacket() {
    return maxChunksPerPacket;
  }

}
