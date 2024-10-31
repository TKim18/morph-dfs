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
package org.apache.hadoop.io.erasurecode.rawcoder.util;

import org.apache.hadoop.classification.InterfaceAudience;

import java.nio.ByteBuffer;

/**
 * Utilities for implementing Reed-Solomon code, used by RS coder. Some of the
 * codes are borrowed from ISA-L implementation (C or ASM codes).
 */
@InterfaceAudience.Private
public final class RSUtil {

  private RSUtil(){}

  // We always use the byte system (with symbol size 8, field size 256,
  // primitive polynomial 285, and primitive root 2).
  public static GaloisField GF = GaloisField.getInstance();
  public static final int PRIMITIVE_ROOT = 2;

  public static int[] getPrimitivePower(int numDataUnits, int numParityUnits) {
    int[] primitivePower = new int[numDataUnits + numParityUnits];
    // compute powers of the primitive root
    for (int i = 0; i < numDataUnits + numParityUnits; i++) {
      primitivePower[i] = GF.power(PRIMITIVE_ROOT, i);
    }
    return primitivePower;
  }

  public static void initTables(int k, int rows, byte[] codingMatrix,
      int matrixOffset, byte[] gfTables) {
    int i, j;

    int offset = 0, idx = matrixOffset;
    for (i = 0; i < rows; i++) {
      for (j = 0; j < k; j++) {
        GF256.gfVectMulInit(codingMatrix[idx++], gfTables, offset);
        offset += 32;
      }
    }
  }

  /**
   * Ported from Intel ISA-L library.
   */
  public static void genCauchyMatrix(byte[] a, int m, int k) {
    // Identity matrix in high position
    for (int i = 0; i < k; i++) {
      a[k * i + i] = 1;
    }

    // For the rest choose 1/(i + j) | i != j
    int pos = k * k;
    for (int i = k; i < m; i++) {
      for (int j = 0; j < k; j++) {
        a[pos++] = GF256.gfInv((byte) (i ^ j));
      }
    }
  }

  /**
   * Dax current WIP:
   *Conversion matrix for typical (non-LRC) conversions
   */

  public static void genCCTranscodingMatrix(byte[] conversionMatrix, int mergeFactor, int parityNum, int initialDataBlockNum){
    for (int i = 0; i < parityNum; i++){
      for (int j = 0; j < mergeFactor; j++){
        //Equivalent to (i * mergeFactor * parityNum) + (j * parityNum) + i, which iterates through rows and blocks and offsets values based on rows
        conversionMatrix[(i * mergeFactor + j) * parityNum + i] = (byte) GF.power(PRIMITIVE_ROOT, initialDataBlockNum * i * j);

      }
    }
  }

  /**
   * Conversion matrix for CC to LRCC conversions.
   */
  public static void genLRCTranscodingMatrix(byte[] conversionMatrix, int mergeFactor, int oldParityNum, int newParityNum, int initialDataBlockNum){
    // only supporting this case for now.
    int newLocalParityNum = mergeFactor;
    for (int i = 0; i < newLocalParityNum; ++i) {
      conversionMatrix[i * mergeFactor * oldParityNum + i * oldParityNum] = 1;
    }

    for (int i = 0; i < newParityNum; i++){
      for (int j = 0; j < mergeFactor; j++){
        //Equivalent to (i * mergeFactor * parityNum) + (j * parityNum) + i, which iterates through rows and blocks and offsets values based on rows
        conversionMatrix[((i+newLocalParityNum) * mergeFactor + j) * oldParityNum + (i+newLocalParityNum) - 1] = (byte) GF.power(PRIMITIVE_ROOT, initialDataBlockNum * (i+1) * j);
      }
    }
  }

  public static void genRSMatrix(byte[] a, int m, int k) {
    // Identity matrix in high position
    for (int i = 0; i < k; i++) {
      a[k * i + i] = 1;
    }

    byte gen = 1;
    byte p;

    for (int i = k; i < m; i++) {
      p = 1;
      for (int j = 0; j < k; j++) {
        a[k * i + j] = p;
        p = GF256.gfMul(p, gen);
      }
      gen = GF256.gfMul(gen, (byte) 2);
    }

  }


  /**
   * Generate a Cauchy matrix for LRC encoding.
   */
  public static void genLRCauchyMatrix(byte[] a, int m, int l, int k) {
    // Identity matrix in high position
    for (int i = 0; i < k; i++) {
      a[k * i + i] = 1;
    }

    int numUnitsInLocalGroup = k/l;
    for (int i = k; i < k+l; ++i) {
      int startPos = (i-k) * numUnitsInLocalGroup;
      for (int j = startPos; j < startPos + numUnitsInLocalGroup; ++j) {
        a[k * i + j] = 1;
      }
    }

    // For the rest choose 1/(i + j) | i != j
    int pos = k * (k+l);
    for (int i = k+l; i < m; i++) {
      for (int j = 0; j < k; j++) {
        a[pos++] = GF256.gfInv((byte) (i ^ j));
      }
    }
  }


  /**
   * Generate a Vandermonde matrix for LRC encoding.
   */
  public static void genLRCMatrix(byte[] a, int m, int l, int k) {
    // Identity matrix in high position
    for (int i = 0; i < k; i++) {
      a[k * i + i] = 1;
    }

    byte gen = 1;
    byte p;
    int numUnitsInLocalGroup = k/l;

    for (int i = k; i < k+l; ++i) {
      p = 1;
      int startPos = (i-k) * numUnitsInLocalGroup;

      for (int j = startPos; j < startPos + numUnitsInLocalGroup; ++j) {
        a[k * i + j] = p;
        p = GF256.gfMul(p, gen);
      }
    }

    gen = GF256.gfMul(gen, (byte) 2);
    for (int i = k+l; i < m; i++) {
      p = 1;
      for (int j = 0; j < k; j++) {
        a[k * i + j] = p;
        p = GF256.gfMul(p, gen);
      }
      gen = GF256.gfMul(gen, (byte) 2);
    }

  }

  public static void transcodeData(){

  }

  /**
   * Encode a group of inputs data and generate the outputs. It's also used for
   * decoding because, in this implementation, encoding and decoding are
   * unified.
   *
   * The algorithm is ported from Intel ISA-L library for compatible. It
   * leverages Java auto-vectorization support for performance.
   */
  public static void encodeData(byte[] gfTables, int dataLen, byte[][] inputs,
      int[] inputOffsets, byte[][] outputs,
      int[] outputOffsets) {
    int numInputs = inputs.length;
    int numOutputs = outputs.length;
    int l, i, j, iPos, oPos;
    byte[] input, output;
    byte s;
    final int times = dataLen / 8;
    final int extra = dataLen - dataLen % 8;
    byte[] tableLine;

    for (l = 0; l < numOutputs; l++) {
      output = outputs[l];

      for (j = 0; j < numInputs; j++) {
        input = inputs[j];
        iPos = inputOffsets[j];
        oPos = outputOffsets[l];

        s = gfTables[j * 32 + l * numInputs * 32 + 1];
        tableLine = GF256.gfMulTab()[s & 0xff];

        /**
         * Purely for performance, assuming we can use 8 bytes in the SIMD
         * instruction. Subject to be improved.
         */
        for (i = 0; i < times; i++, iPos += 8, oPos += 8) {
          output[oPos + 0] ^= tableLine[0xff & input[iPos + 0]];
          output[oPos + 1] ^= tableLine[0xff & input[iPos + 1]];
          output[oPos + 2] ^= tableLine[0xff & input[iPos + 2]];
          output[oPos + 3] ^= tableLine[0xff & input[iPos + 3]];
          output[oPos + 4] ^= tableLine[0xff & input[iPos + 4]];
          output[oPos + 5] ^= tableLine[0xff & input[iPos + 5]];
          output[oPos + 6] ^= tableLine[0xff & input[iPos + 6]];
          output[oPos + 7] ^= tableLine[0xff & input[iPos + 7]];
        }

        /**
         * For the left bytes, do it one by one.
         */
        for (i = extra; i < dataLen; i++, iPos++, oPos++) {
          output[oPos] ^= tableLine[0xff & input[iPos]];
        }
      }
    }
  }

  /**
   * See above. Try to use the byte[] version when possible.
   */
  public static void encodeData(byte[] gfTables, ByteBuffer[] inputs,
      ByteBuffer[] outputs) {
    int numInputs = inputs.length;
    int numOutputs = outputs.length;
    //Debugging
//    System.out.println("NumInputs and Outputs: " + numInputs+" "+numOutputs);
    int dataLen = inputs[0].remaining();
    int l, i, j, iPos, oPos;
    ByteBuffer input, output;
    byte s;
    final int times = dataLen / 8;
    final int extra = dataLen - dataLen % 8;
    byte[] tableLine;

    for (l = 0; l < numOutputs; l++) {
      output = outputs[l];

      for (j = 0; j < numInputs; j++) {
        input = inputs[j];
        iPos = input.position();
        oPos = output.position();

        s = gfTables[j * 32 + l * numInputs * 32 + 1];
        tableLine = GF256.gfMulTab()[s & 0xff];

        for (i = 0; i < times; i++, iPos += 8, oPos += 8) {
          output.put(oPos + 0, (byte) (output.get(oPos + 0) ^
              tableLine[0xff & input.get(iPos + 0)]));
          output.put(oPos + 1, (byte) (output.get(oPos + 1) ^
              tableLine[0xff & input.get(iPos + 1)]));
          output.put(oPos + 2, (byte) (output.get(oPos + 2) ^
              tableLine[0xff & input.get(iPos + 2)]));
          output.put(oPos + 3, (byte) (output.get(oPos + 3) ^
              tableLine[0xff & input.get(iPos + 3)]));
          output.put(oPos + 4, (byte) (output.get(oPos + 4) ^
              tableLine[0xff & input.get(iPos + 4)]));
          output.put(oPos + 5, (byte) (output.get(oPos + 5) ^
              tableLine[0xff & input.get(iPos + 5)]));
          output.put(oPos + 6, (byte) (output.get(oPos + 6) ^
              tableLine[0xff & input.get(iPos + 6)]));
          output.put(oPos + 7, (byte) (output.get(oPos + 7) ^
              tableLine[0xff & input.get(iPos + 7)]));
        }

        for (i = extra; i < dataLen; i++, iPos++, oPos++) {
          output.put(oPos, (byte) (output.get(oPos) ^
              tableLine[0xff & input.get(iPos)]));
        }
      }
    }
  }

  //Testing for avoiding CCTranscode Out of Bounds Exception
  public static void transcodeData(byte[] gfTables, int mergeFactor, ByteBuffer[] inputs,
                                ByteBuffer[] outputs) {
    int numInputs = inputs.length;
    int numOutputs = outputs.length;
    //Debugging
//    System.out.println("NumInputs and Outputs: " + numInputs+" "+numOutputs);
    int dataLen = inputs[0].remaining();
    int l, i, j, iPos, oPos;
    ByteBuffer input, output;
    byte s;
    final int times = dataLen / 8;
    final int extra = dataLen - dataLen % 8;
    byte[] tableLine;

    for (l = 0; l < numOutputs; l++) {
      output = outputs[l];

      for (j = 0; j < numInputs; j++) {
        input = inputs[j];
        iPos = input.position();
        oPos = output.position();

        s = gfTables[j * 32 + l * numInputs * 32 + 1];
        tableLine = GF256.gfMulTab()[s & 0xff];

        for (i = 0; i < times; i++, iPos += 8, oPos += 8) {
          output.put(oPos + 0, (byte) (output.get(oPos + 0) ^
                  tableLine[0xff & input.get(iPos + 0)]));
          output.put(oPos + 1, (byte) (output.get(oPos + 1) ^
                  tableLine[0xff & input.get(iPos + 1)]));
          output.put(oPos + 2, (byte) (output.get(oPos + 2) ^
                  tableLine[0xff & input.get(iPos + 2)]));
          output.put(oPos + 3, (byte) (output.get(oPos + 3) ^
                  tableLine[0xff & input.get(iPos + 3)]));
          output.put(oPos + 4, (byte) (output.get(oPos + 4) ^
                  tableLine[0xff & input.get(iPos + 4)]));
          output.put(oPos + 5, (byte) (output.get(oPos + 5) ^
                  tableLine[0xff & input.get(iPos + 5)]));
          output.put(oPos + 6, (byte) (output.get(oPos + 6) ^
                  tableLine[0xff & input.get(iPos + 6)]));
          output.put(oPos + 7, (byte) (output.get(oPos + 7) ^
                  tableLine[0xff & input.get(iPos + 7)]));
        }

        for (i = extra; i < dataLen; i++, iPos++, oPos++) {
          output.put(oPos, (byte) (output.get(oPos) ^
                  tableLine[0xff & input.get(iPos)]));
        }
      }
    }
  }

}
