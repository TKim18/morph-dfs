package org.apache.hadoop.hdfs.server.datanode.erasurecode.transcoder;

import java.io.IOException;
import java.nio.ByteBuffer;

public class StripedBlockTranscoder extends StripedTranscoder
    implements Runnable {

  StripedBlockTranscoder(ErasureTranscodingWorker worker, StripedTranscodingInfo info) {
    super(worker, info);
  }

  @Override
  public void run() {
    // execute transcode
    try {
      if (inputMode == 1) {
        this.merger.init();

        this.merger.merge();
      } else {
        this.reader.init();

        this.writer.init();

        transcode();

        this.writer.endTargetBlocks();
      }
    } catch (Throwable e) {
      LOG.warn("Failed to transcode striped block", e);
    } finally {
      if (inputMode > 0) {
        this.transcoder.release(); // add remove on decode
      } else {
        this.reader.close();
        this.writer.close();
        this.decoder.release();
      }
    }
  }

  @Override
  void transcode() throws IOException {
    long totalReadTime = 0;
    long totalTranscodeTime = 0;
    long totalWriteTime = 0;

    while (positionInBlock < maxPositionInBlock) {
      long remaining = maxPositionInBlock - positionInBlock;
      int toTranscodeLen = (int) Math.min(reader.getBufferSize(), remaining);
      if (inputMode == 2) {
        toTranscodeLen = (int) remaining;
      }

      final long start = System.currentTimeMillis();
      // step 1: read from all sources be it data or parity blocks
      reader.readSources(toTranscodeLen);

      final long readEnd = System.currentTimeMillis();
      // step 2: transcode source data into output parity data
      transcodeTargets(toTranscodeLen);

      final long transcodeEnd = System.currentTimeMillis();
      // step 3: write out new parities to targets
      writer.transferData2Targets();

      final long writeEnd = System.currentTimeMillis();

      totalReadTime += readEnd - start;
      totalTranscodeTime += transcodeEnd - readEnd;
      totalWriteTime += writeEnd - transcodeEnd;

      clearBuffers();
      positionInBlock += toTranscodeLen;
    }

    LOG.info("Transcode read time = {}, transition time = {}, transcode write time = {}",
            totalReadTime, totalTranscodeTime, totalWriteTime);
  }

  private void transcodeTargets(int toTranscodeLen) throws IOException {
    ByteBuffer[] inputs;
    if (inputMode == 0) {
      inputs = reader.getInputBuffers4NaiveTranscode(toTranscodeLen);
    } else if (inputMode == 1) {
      inputs = reader.getInputBuffers4Transcode(toTranscodeLen);
    } else {
      assert (inputMode == 2);
      inputs = reader.getInput4PiggybackedTranscode(toTranscodeLen);
    }
    ByteBuffer[] outputs = writer.getTargetBuffers(toTranscodeLen);

    final long start = System.currentTimeMillis();
    if (this.inputMode == 1) {
      transcode(inputs, outputs);
    } else if (this.inputMode == 0) {
      decode(inputs, erasedIndices, outputs);
    } else if (this.inputMode == 2) {
      transcode(inputs, outputs);
    }
    final long end = System.currentTimeMillis();
    LOG.info("TRANSCODE TIME = " + (end - start));

    writer.updateTargetBuffers(toTranscodeLen);
  }

  private void transcode(ByteBuffer[] inputs, ByteBuffer[] outputs) throws IOException {
    this.transcoder.transcode(inputs, outputs);
  }

  private void decode(ByteBuffer[] inputs, int[] erasedIndices, ByteBuffer[] outputs) throws IOException {
    this.decoder.decode(inputs, erasedIndices, outputs);
  }

  private void clearBuffers() {
    this.reader.clearBuffers();
    this.writer.clearBuffers();
  }
}
