package org.apache.hadoop.io.erasurecode.rawcoder;

import java.nio.ByteBuffer;

public class ByteBufferUpdatingState extends ByteBufferEncodingState {

  final int offsetInGroup;
  final int length;

  ByteBufferUpdatingState(RawErasureEncoder encoder,
                          ByteBuffer[] inputs,
                          ByteBuffer[] outputs,
                          int offsetInGroup, int length) {
    super(encoder, inputs, outputs);
    this.offsetInGroup = offsetInGroup;
    this.length = length;
  }

}
