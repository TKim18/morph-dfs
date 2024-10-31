package org.apache.hadoop.hdfs.server.datanode.erasurecode.transcoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureTranscoder;
import org.apache.hadoop.io.erasurecode.rawcoder.TranscoderOptions;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletionService;

abstract class StripedTranscoder {
  protected static final Logger LOG = DataNode.LOG;

  // setup fields
  private final ErasureTranscodingWorker worker;
  private final Configuration conf;
  private final DataNode datanode;

  // general utility helpers
  private final CachingStrategy cachingStrategy;
  private TranscoderOptions transcoderOptions;
  protected RawErasureTranscoder transcoder;
  private ErasureCoderOptions decoderOptions;
  protected RawErasureDecoder decoder;
  final StripedReader reader;
  final StripedWriter writer;
  final StripedMerger merger;

  // task specific info
  private final ErasureCodingPolicy ecPolicy;
  // 0:
  protected final byte inputMode;
  protected int[] erasedIndices;

  //Old schema to obtain old data block and parity nlock numbers
  private final ECSchema oldSchema;

  // transcoder/read/write state
  protected long positionInBlock;
  protected long maxPositionInBlock;

  private static final ByteBufferPool BUFFER_POOL = new ElasticByteBufferPool();

  StripedTranscoder(ErasureTranscodingWorker worker,
                    StripedTranscodingInfo info) {
    this.worker = worker;
    this.datanode = worker.getDatanode();
    this.conf = worker.getConf();

    this.ecPolicy = info.getEcPolicy();
    this.oldSchema = info.getOldSchema();

    this.cachingStrategy = CachingStrategy.newDefaultStrategy();
    this.inputMode = info.getInputMode();
    if (this.inputMode > 0) {
      this.transcoderOptions = new TranscoderOptions(
              this.ecPolicy.getNumDataUnits(),
              this.ecPolicy.getNumParityUnits(), this.oldSchema);
      this.transcoder = CodecUtil.createRawTranscoder(conf,
              ecPolicy.getCodecName(), transcoderOptions);
      this.merger = new StripedMerger(this, datanode, conf, info);
    } else {
      this.erasedIndices = getErasedIndices();
      this.decoderOptions = new ErasureCoderOptions(
              ecPolicy.getNumDataUnits(), ecPolicy.getNumLocalParityUnits(), ecPolicy.getNumParityUnits());
      this.decoder = CodecUtil.createRawDecoder(conf, ecPolicy.getCodecName(),
              decoderOptions);
      this.merger = null;
    }
    this.reader = new StripedReader(this, datanode, conf, info);
    this.writer = new StripedWriter(this, datanode, conf, info);
  }

  abstract void transcode() throws IOException;

  boolean useDirectBuffer() {
    if (inputMode > 0) {
      return transcoder.preferDirectBuffer();
    } else {
      return decoder.preferDirectBuffer();
    }
  }

  ByteBuffer allocateBuffer(int length) {
    return allocateBuffer(useDirectBuffer(), length);
  }

  ByteBuffer allocateBuffer(boolean directBuffer, int length) {
    return BUFFER_POOL.getBuffer(directBuffer, length);
  }

  void releaseBuffer(ByteBuffer buffer) {
    BUFFER_POOL.putBuffer(buffer);
  }

  CachingStrategy getCachingStrategy() {
    return cachingStrategy;
  }

  CompletionService<StripedBlockUtil.BlockReadStats> createReadService() {
    return worker.createReadService();
  }

  int[] getErasedIndices() {
    // will always be the parities after data in new ecpolicy
    int[] erased = new int[ecPolicy.getNumParityUnits() + ecPolicy.getNumLocalParityUnits()];
    for (int i = 0; i < erased.length; i++) {
      erased[i] = ecPolicy.getNumDataUnits() + i;
    }
    return erased;
  }

  public int getNumNewParities() {
    return ecPolicy.getNumParityUnits();
  }

  public int getNumNewLocalParities() {
    return ecPolicy.getNumLocalParityUnits();
  }

  public ErasureCodingPolicy getECPolicy() {
    return this.ecPolicy;
  }
}
