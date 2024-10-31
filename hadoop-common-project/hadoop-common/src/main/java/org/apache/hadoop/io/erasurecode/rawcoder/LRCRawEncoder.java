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

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.util.DumpUtil;
import org.apache.hadoop.io.erasurecode.rawcoder.util.RSUtil;

/**
 * A raw erasure encoder in Local Reconstruction code scheme.
 */
@InterfaceAudience.Private
public class LRCRawEncoder extends RawErasureEncoder {
    // relevant to schema and won't change during encode calls.
    private byte[] encodeMatrix;
    /**
     * Array of input tables generated from coding coefficients previously.
     * Must be of size 32*k*rows
     */
    private byte[] gfTables;

    public LRCRawEncoder(ErasureCoderOptions coderOptions) {
        super(coderOptions);
        LOG.info("Encoding with LRCRawEncoder");

        if (getNumAllUnits() >= RSUtil.GF.getFieldSize()) {
            throw new HadoopIllegalArgumentException(
                    "Invalid numDataUnits and numParityUnits");
        }

        encodeMatrix = new byte[getNumAllUnits() * getNumDataUnits()];
        RSUtil.genLRCMatrix(encodeMatrix, getNumAllUnits(), getNumLocalParityUnits(), getNumDataUnits());
        if (allowVerboseDump()) {
            DumpUtil.dumpMatrix(encodeMatrix, getNumDataUnits(), getNumAllUnits());
        }
        int numAllUnits = getNumAllUnits();
        gfTables = new byte[getNumAllUnits() * getNumDataUnits() * 32];
        RSUtil.initTables(getNumDataUnits(), getNumLocalParityUnits() + getNumParityUnits(), encodeMatrix,
                getNumDataUnits() * getNumDataUnits(), gfTables);
        if (allowVerboseDump()) {
            System.out.println(DumpUtil.bytesToHex(gfTables, -1));
        }
    }

    @Override
    protected void doEncode(ByteBufferEncodingState encodingState) {
        CoderUtil.resetOutputBuffers(encodingState.outputs,
                encodingState.encodeLength);
        RSUtil.encodeData(gfTables, encodingState.inputs, encodingState.outputs);
    }

    @Override
    protected void doEncode(ByteArrayEncodingState encodingState) {
        CoderUtil.resetOutputBuffers(encodingState.outputs,
                encodingState.outputOffsets,
                encodingState.encodeLength);
        RSUtil.encodeData(gfTables, encodingState.encodeLength,
                encodingState.inputs,
                encodingState.inputOffsets, encodingState.outputs,
                encodingState.outputOffsets);
    }
}
