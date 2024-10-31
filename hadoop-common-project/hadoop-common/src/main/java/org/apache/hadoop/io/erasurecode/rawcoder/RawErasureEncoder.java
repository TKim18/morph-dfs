/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ECChunk;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.util.DumpUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * An abstract raw erasure encoder that's to be inherited by new encoders.
 *
 * Raw erasure coder is part of erasure codec framework, where erasure coder is
 * used to encode/decode a group of blocks (BlockGroup) according to the codec
 * specific BlockGroup layout and logic. An erasure coder extracts chunks of
 * data from the blocks and can employ various low level raw erasure coders to
 * perform encoding/decoding against the chunks.
 *
 * To distinguish from erasure coder, here raw erasure coder is used to mean the
 * low level constructs, since it only takes care of the math calculation with
 * a group of byte buffers.
 *
 * Note it mainly provides encode() calls, which should be stateless and may be
 * made thread-safe in future.
 */
@InterfaceAudience.Private
public abstract class RawErasureEncoder {

  private final ErasureCoderOptions coderOptions;

  static final Logger LOG = LoggerFactory.getLogger(RawErasureEncoder.class);

  public RawErasureEncoder(ErasureCoderOptions coderOptions) {
    this.coderOptions = coderOptions;
  }

  /**
   * Encode with inputs and generates outputs.
   *
   * Note, for both inputs and outputs, no mixing of on-heap buffers and direct
   * buffers are allowed.
   *
   * If the coder option ALLOW_CHANGE_INPUTS is set true (false by default), the
   * content of input buffers may change after the call, subject to concrete
   * implementation. Anyway the positions of input buffers will move forward.
   *
   * @param inputs input buffers to read data from. The buffers' remaining will
   *               be 0 after encoding
   * @param outputs output buffers to put the encoded data into, ready to read
   *                after the call
   * @throws IOException if the encoder is closed.
   */
  public void encode(ByteBuffer[] inputs, ByteBuffer[] outputs)
      throws IOException {
    ByteBufferEncodingState bbeState = new ByteBufferEncodingState(
        this, inputs, outputs);

    LOG.info("[BWOCC] encode() with ByteBuffer[]; outputs.length = {}; encodelength = {}; outputs[0].position() = {}; usingDirectBuffer = {}", outputs.length, bbeState.encodeLength, outputs[0].position(), bbeState.usingDirectBuffer);
    boolean usingDirectBuffer = bbeState.usingDirectBuffer;
    int dataLen = bbeState.encodeLength;
    if (dataLen == 0) {
      return;
    }

    int[] inputPositions = new int[inputs.length];
    for (int i = 0; i < inputPositions.length; i++) {
      if (inputs[i] != null) {
        inputPositions[i] = inputs[i].position();
      }
    }

    if (usingDirectBuffer) {
      doEncode(bbeState);
    } else {
      ByteArrayEncodingState baeState = bbeState.convertToByteArrayState();
      doEncode(baeState);
    }

    for (int i = 0; i < inputs.length; i++) {
      if (inputs[i] != null) {
        // dataLen bytes consumed
        inputs[i].position(inputPositions[i] + dataLen);
      }
    }

  }


  public void encode(ByteBuffer[] inputs, ByteBuffer[] outputs, int atRow)
          throws IOException {
    LOG.debug("[BWOCC] encode() with ByteBuffer[] with atRow = {}", atRow);
    ByteBufferEncodingState bbeState = new ByteBufferEncodingState(
            this, inputs, outputs);

    boolean usingDirectBuffer = bbeState.usingDirectBuffer;
    int dataLen = bbeState.encodeLength;
    if (dataLen == 0) {
      return;
    }

    int[] inputPositions = new int[inputs.length];
    for (int i = 0; i < inputPositions.length; i++) {
      if (inputs[i] != null) {
        inputPositions[i] = inputs[i].position();
      }
    }

    if (usingDirectBuffer) {
      doEncode(bbeState);
    } else {
      ByteArrayEncodingState baeState = bbeState.convertToByteArrayState();
      doEncode(baeState, atRow);
    }

    for (int i = 0; i < inputs.length; i++) {
      if (inputs[i] != null) {
        // dataLen bytes consumed
        inputs[i].position(inputPositions[i] + dataLen);
      }
    }
  }

  /**
   * Perform the real encoding work using direct ByteBuffer.
   * @param encodingState the encoding state
   */
  protected abstract void doEncode(ByteBufferEncodingState encodingState)
      throws IOException;

  /**
   * Encode with inputs and generates outputs. More see above.
   *
   * @param inputs input buffers to read data from
   * @param outputs output buffers to put the encoded data into, read to read
   *                after the call
   */
  public void encode(byte[][] inputs, byte[][] outputs) throws IOException {
    ByteArrayEncodingState baeState = new ByteArrayEncodingState(
        this, inputs, outputs);

    LOG.info("[BWOCC] encode() with byte[][]");
    int dataLen = baeState.encodeLength;
    if (dataLen == 0) {
      return;
    }

    doEncode(baeState);
  }

  /**
   * Perform the real encoding work using bytes array, supporting offsets
   * and lengths.
   * @param encodingState the encoding state
   */
  protected abstract void doEncode(ByteArrayEncodingState encodingState)
      throws IOException;

  protected void doEncode(ByteArrayEncodingState encodingState, int atRow)
    throws IOException {

  }

  /**
   * Encode with inputs and generates outputs. More see above.
   *
   * @param inputs input buffers to read data from
   * @param outputs output buffers to put the encoded data into, read to read
   *                after the call
   * @throws IOException if the encoder is closed.
   */
  public void encode(ECChunk[] inputs, ECChunk[] outputs) throws IOException {
    LOG.info("[BWOCC] encode() with ECChunk[]");
    ByteBuffer[] newInputs = ECChunk.toBuffers(inputs);
    ByteBuffer[] newOutputs = ECChunk.toBuffers(outputs);
    encode(newInputs, newOutputs);
  }

  public void update(ByteBuffer[] inputs, ByteBuffer[] outputs, int offset, int length) throws IOException {
    ByteBufferUpdatingState bbeState = new ByteBufferUpdatingState(
      this, inputs, outputs, offset, length);

    boolean usingDirectBuffer = bbeState.usingDirectBuffer;
    int dataLen = bbeState.encodeLength;
    if (dataLen == 0) {
      return;
    }

    int[] inputPositions = new int[inputs.length];
    for (int i = 0; i < inputPositions.length; i++) {
      if (inputs[i] != null) {
        inputPositions[i] = inputs[i].position();
      }
    }

    if (usingDirectBuffer) {
      doUpdate(bbeState);
    } else {
      LOG.error("Can't update non-direct buffer");
    }

    for (int i = 0; i < inputs.length; i++) {
      if (inputs[i] != null) {
        // dataLen bytes consumed
        inputs[i].position(inputPositions[i] + dataLen);
      }
    }
  }

  protected void doUpdate(ByteBufferUpdatingState updatingState)
    throws IOException {
    // NO-OP by default
  }

  public int getNumDataUnits() {
    return coderOptions.getNumDataUnits();
  }

  public int getNumParityUnits() {
    return coderOptions.getNumParityUnits();
  }

  public int getNumAllUnits() {
    return coderOptions.getNumAllUnits();
  }

  public int getNumDataUnitsFinal() { return coderOptions.getNumDataUnitsFinal(); }
  public int getNumParityUnitsFinal() { return coderOptions.getNumParityUnitsFinal(); }
  public int getNumLocalParityUnits() { return coderOptions.getNumLocalParityUnits(); }

  /**
   * Tell if direct buffer is preferred or not. It's for callers to
   * decide how to allocate coding chunk buffers, using DirectByteBuffer or
   * bytes array. It will return false by default.
   * @return true if native buffer is preferred for performance consideration,
   * otherwise false.
   */
  public boolean preferDirectBuffer() {
    return false;
  }

  /**
   * Allow change into input buffers or not while perform encoding/decoding.
   * @return true if it's allowed to change inputs, false otherwise
   */
  public boolean allowChangeInputs() {
    return coderOptions.allowChangeInputs();
  }

  /**
   * Allow to dump verbose info during encoding/decoding.
   * @return true if it's allowed to do verbose dump, false otherwise.
   */
  public boolean allowVerboseDump() {
    return coderOptions.allowVerboseDump();
  }

  /**
   * Should be called when release this coder. Good chance to release encoding
   * or decoding buffers
   */
  public void release() {
    // Nothing to do here.
  }
}
