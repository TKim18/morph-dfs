package org.apache.hadoop.hdfs.server.datanode.erasurecode.transcoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient.CorruptedBlocks;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.hdfs.util.StripedBlockUtil.StripingChunkReadResult;
import org.apache.hadoop.util.DataChecksum;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

class StripedReader {
  private static final Logger LOG = DataNode.LOG;

  private final StripedTranscoder transcoder;
  private final DataNode datanode;
  private final Configuration conf;

  // source info
  private final byte inputMode;
  private final ExtendedBlock sourceBlock;
  private final DatanodeInfo[] sources; // TODO: won't be final later when this can change for back-ups
  private final int[] sourceBlockIdOffsets; // block id's of sources
  private final long[] sourceBlockLengths; // block len's of sources
  private final long maxBlockLength;

  private final int numParities;
  private final int numNewParities;
  private final int numNewLocalParities;
  private int subPacketSize;
  private int blockSize;
  private int subBlockSize;

  // reading configs/objs
  private final int readTimeoutMillis;
  private final int readBufferSize;
  private DataChecksum checksum;
  private int bufferSize;
  private final Map<Future<StripedBlockUtil.BlockReadStats>, Integer> futures = new HashMap<>();
  private final CompletionService<StripedBlockUtil.BlockReadStats> readService;
  private final StripedBlockReader[] readers;
  private int numData;

  StripedReader(StripedTranscoder transcoder, DataNode datanode,
                Configuration conf, StripedTranscodingInfo transcoderInfo) {
    this.transcoder = transcoder;
    this.datanode = datanode;
    this.conf = conf;

    // get conf values for read timeout and buffer sizes
    this.readTimeoutMillis = conf.getInt(
        DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_TIMEOUT_MILLIS_KEY,
        DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_TIMEOUT_MILLIS_DEFAULT);
    this.readBufferSize = conf.getInt(
        DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_BUFFER_SIZE_KEY,
        DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_BUFFER_SIZE_DEFAULT);

    this.inputMode = transcoderInfo.getInputMode();
    this.numParities = transcoderInfo.getOldSchema().getNumParityUnits();
    this.numNewParities = transcoder.getNumNewParities();
    this.numNewLocalParities = transcoder.getNumNewLocalParities();
    subPacketSize = numNewParities;
    // get blockSize from configurations
    blockSize = conf.getInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, (int) DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);
    LOG.debug("[BWOCC]-t StripedReader - Block size: " + blockSize);
    subBlockSize = blockSize / subPacketSize;

    if (this.inputMode == 1) {
      // parse source block and id offsets based on list of parity blocks and ec scheme
      this.sourceBlock = transcoderInfo.getParityBlocks()[0];
      ExtendedBlock dataSourceBlock = transcoderInfo.getDataBlocks();
      this.sources = transcoderInfo.getParitySources();
      this.sourceBlockIdOffsets = new int[sources.length];
      this.sourceBlockLengths = new long[sources.length];
      int i = 0;
      long id = this.sourceBlock.getBlockId();
      for (ExtendedBlock block : transcoderInfo.getParityBlocks()) {
        long length = block.getNumBytes();
        for (int p = 0; p < numParities; p++) {
          this.sourceBlockIdOffsets[i] = (int) ((block.getBlockId() + p) - id);
          this.sourceBlockLengths[i] = length / numParities;
          i++;
        }
      }
    } else if (this.inputMode == 2) {
      this.numData = transcoderInfo.getEcPolicy().getNumDataUnits();
      // this.sourceBlock = transcoderInfo.getParityBlocks()[0];
      this.sourceBlock = transcoderInfo.getDataBlocks();
      // long dataSourceBlockId = transcoderInfo.getDataBlocks().getBlockId();
      this.sources = new DatanodeInfo[transcoderInfo.getDataSources().length + transcoderInfo.getParitySources().length];
      System.arraycopy(transcoderInfo.getDataSources(), 0, this.sources, 0, transcoderInfo.getDataSources().length);
      System.arraycopy(transcoderInfo.getParitySources(), 0, this.sources, transcoderInfo.getDataSources().length, transcoderInfo.getParitySources().length);
      this.sourceBlockIdOffsets = new int[sources.length];
      this.sourceBlockLengths = new long[sources.length];
      System.arraycopy(transcoderInfo.getDataIdOffsets(), 0, this.sourceBlockIdOffsets, 0, numData);
      for (int i = 0; i < numData; ++i) {
        this.sourceBlockLengths[i] = (long) subBlockSize * (numNewParities - numParities);
      }
      int i;
      long id = this.sourceBlock.getBlockId();
      i = numData;
      for (ExtendedBlock block : transcoderInfo.getParityBlocks()) {
        long length = block.getNumBytes();
        for (int p = 0; p < numParities; p++) {
          this.sourceBlockIdOffsets[i] = (short) ((block.getBlockId() + p) - id);
          this.sourceBlockLengths[i] = length / numParities;
          i++;
        }
      }
    } else {
      int numData = transcoderInfo.getEcPolicy().getNumDataUnits();
      this.sourceBlock = transcoderInfo.getDataBlocks();
      this.sources = transcoderInfo.getDataSources();
      this.sourceBlockIdOffsets = Arrays.copyOf(transcoderInfo.getDataIdOffsets(), numData);
      this.sourceBlockLengths = transcoderInfo.getDataLengths();
    }
    this.readers = new StripedBlockReader[sources.length];
    this.readService = transcoder.createReadService();
    this.maxBlockLength = getMaxBlockLength();
  }

  void init() throws IOException {
    initReaders();

    initBufferSize();

    initMaxLength();
  }

  private void initReaders() throws IOException {
    if (this.inputMode == 1) {
      boolean success = initParityReaders();

      if (!success) {
        LOG.error("Unable to init all readers, ending");
        // TODO: fix this path of using data as back-up
        // initDataReaders();
      }
    } else if (this.inputMode == 2) {
      boolean success = initDataAndParityReaders();

      if (!success) {
        LOG.error("Unable to init all readers, ending");
        // TODO: fix this path of using data as back-up
        // initDataReaders();
      }
    } else {
      initDataReaders();
    }
  }

  private void initDataReaders() throws IOException {
    StripedBlockReader reader;
    for (short i = 0; i < sources.length; i++) {
      reader = createReader(i, 0);
      readers[i] = reader;

      // require all sources to succeed
      if (reader.getBlockReader() != null) {
        initOrVerifyChecksum(reader);
      } else {
        throw new IOException("Unable to read " +
            "from all data sources to transcode");
      }
    }
  }

  private boolean initParityReaders() {
    StripedBlockReader reader;
    for (short i = 0; i < sources.length; i++) {
      reader = createReader(i, 0);
      readers[i] = reader;

      // require all sources to succeed
      if (reader.getBlockReader() != null) {
        initOrVerifyChecksum(reader);
      } else {
        LOG.info("Unable to read from all " +
            "parities to use convertible codes");
        return false;
      }
    }
    return true;
  }

  private boolean initDataAndParityReaders() {
    StripedBlockReader reader;

    for (short i = 0; i < sources.length; i++) {
      int offset = 0;
      if (i < numData) {
        offset = subBlockSize * numParities;
      }
      reader = createReader(i, offset);
      readers[i] = reader;

      // require all sources to succeed
      if (reader.getBlockReader() != null) {
        initOrVerifyChecksum(reader);
      } else {
        LOG.info("Unable to read from all data + parities " +
                "to use Bandwidth optimal convertible codes");
        return false;
      }
    }
    return true;
  }

  private void initOrVerifyChecksum(StripedBlockReader reader) {
    if (checksum == null) {
      checksum = reader.getBlockReader().getDataChecksum();
    } else {
      assert reader.getBlockReader().getDataChecksum().equals(checksum);
    }
  }

  private StripedBlockReader createReader(short index, int offset) {
    ExtendedBlock b = new ExtendedBlock(sourceBlock);
    b.setBlockId(sourceBlock.getBlockId() + sourceBlockIdOffsets[index]);
    return new StripedBlockReader(
        this, datanode, conf, index,
        b, sources[index], offset,
        transcoder.getCachingStrategy());
  }

  private void initBufferSize() {
    int bytesPerChecksum = checksum.getBytesPerChecksum();
    // The bufferSize is flat to divide bytesPerChecksum
    int readBufferSize = this.readBufferSize;
    bufferSize = readBufferSize < bytesPerChecksum ? bytesPerChecksum :
        readBufferSize - readBufferSize % bytesPerChecksum;
  }

  private void initMaxLength() {
    long maxLength = 0;
    for (long len : sourceBlockLengths) {
      if (len > maxLength) {
        maxLength = len;
      }
    }
    transcoder.maxPositionInBlock = maxLength;
  }

  private int getReadLength(int index, int toTranscodeLen) {
    // the reading length should not exceed the length for reconstruction
    long blockLen = sourceBlockLengths[index];
    long remaining = blockLen - transcoder.positionInBlock;
    return (int) Math.min(remaining, toTranscodeLen);
  }

  ByteBuffer[] getInputBuffers4Transcode(int toTranscodeLen) {
    ByteBuffer[] inputs = new ByteBuffer[sources.length];
    for (int i = 0; i < sources.length; i++) {
      ByteBuffer buffer = readers[i].getReadBuffer();
      paddingBufferToLen(buffer, toTranscodeLen);
      inputs[readers[i].getIndex()] = (ByteBuffer) buffer.flip();
    }
    return inputs;
  }

  ByteBuffer[] getInputBuffers4NaiveTranscode(int toTranscodeLen) {
    ByteBuffer[] inputs = new ByteBuffer[sources.length + this.numNewParities + this.numNewLocalParities];
    for (int i = 0; i < sources.length; i++) {
      ByteBuffer buffer = readers[i].getReadBuffer();
      paddingBufferToLen(buffer, toTranscodeLen);
      inputs[readers[i].getIndex()] = (ByteBuffer) buffer.flip();
    }
    return inputs;
  }


  ByteBuffer[] getInput4PiggybackedTranscode(int toTranscodeLen) {
    ByteBuffer[] inputs = new ByteBuffer[sources.length];
    for (int i = 0; i < sources.length; i++) {
      ByteBuffer buffer = readers[i].getReadBuffer();
      paddingBufferToLen(buffer, toTranscodeLen);
      inputs[readers[i].getIndex()] = (ByteBuffer) buffer.flip();
    }
    return inputs;
  }

  private void paddingBufferToLen(ByteBuffer buffer, int len) {
    if (len > buffer.limit()) {
      buffer.limit(len);
    }
    int toPadding = len - buffer.position();
    for (int i = 0; i < toPadding; i++) {
      buffer.put((byte) 0);
    }
  }

  /**
   * Read all source DNs for transcoding
   *
   * @param toTranscodeLen length to reconstruct
   * @throws IOException when unable to read all sources
   */
  void readSources(int toTranscodeLen) throws IOException {
    CorruptedBlocks corruptedBlocks = new CorruptedBlocks();
    try {
      doReadSources(toTranscodeLen, corruptedBlocks);
    } finally {
      // report corrupted blocks to NN
      datanode.reportCorruptedBlocks(corruptedBlocks);
    }
  }

  private void doReadSources(int toTranscodeLen,
                      CorruptedBlocks corruptedBlocks) throws IOException {
    StripedBlockReader reader;
    for (int i = 0; i < sources.length; i++) {
      reader = readers[i];
      int toRead = getReadLength(i, toTranscodeLen);
      if (toRead > 0) {
        Callable<StripedBlockUtil.BlockReadStats> readCallable =
            reader.readFromBlock(toRead, corruptedBlocks);
        Future<StripedBlockUtil.BlockReadStats> f = readService.submit(readCallable);
        futures.put(f, i);
      } else {
        reader.getReadBuffer().position(0);
      }
    }

    boolean success = true;
    while (!futures.isEmpty()) {
      try {
        StripingChunkReadResult result =
            StripedBlockUtil.getNextCompletedStripedRead(
                readService, futures, readTimeoutMillis);

        if (result.state != StripingChunkReadResult.SUCCESSFUL) {
          // unable to tolerate one failure currently
          cancelReads(futures.keySet());
          clearFuturesAndService();
          success = false;
          break;
        }
      } catch (InterruptedException ie) {
        cancelReads(futures.keySet());
        clearFuturesAndService();
        break;
      }
    }

    if (!success) {
      throw new IOException("Unable to read data " +
          "from all required sources for transcoding");
    }
  }

  // Cancel all reads.
  private static void cancelReads(Collection<Future<StripedBlockUtil.BlockReadStats>> futures) {
    for (Future<StripedBlockUtil.BlockReadStats> future : futures) {
      future.cancel(true);
    }
  }

  // remove all stale futures from readService, and clear futures.
  private void clearFuturesAndService() {
    while (!futures.isEmpty()) {
      try {
        Future<StripedBlockUtil.BlockReadStats> future = readService.poll(
            readTimeoutMillis, TimeUnit.MILLISECONDS
        );
        futures.remove(future);
      } catch (InterruptedException e) {
        LOG.info("Clear stale futures from service is interrupted.", e);
      }
    }
  }

  void clearBuffers() {
    for (StripedBlockReader reader : readers) {
      if (reader.getReadBuffer() != null) {
        reader.getReadBuffer().clear();
      }
    }
  }

  void close() {
    for (StripedBlockReader reader : readers) {
      reader.closeBlockReader();
      transcoder.releaseBuffer(reader.getReadBuffer());
      reader.freeReadBuffer();
    }
  }

  protected ByteBuffer allocateReadBuffer() {
    return transcoder.allocateBuffer(bufferSize);
  }


  // fields useful for the writer as well, could be moved to transcoder
  DataChecksum getChecksum() {
    return checksum;
  }

  int getBufferSize() {
    return bufferSize;
  }

  long getTargetLen() {
    return this.maxBlockLength;
  }

  long getMaxBlockLength() {
    long m = 0L;
    for (long l : sourceBlockLengths) {
      m = Math.max(m, l);
    }
    return m;
  }
}
