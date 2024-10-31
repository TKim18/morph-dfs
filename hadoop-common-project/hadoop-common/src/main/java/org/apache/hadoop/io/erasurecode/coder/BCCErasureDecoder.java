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
package org.apache.hadoop.io.erasurecode.coder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ECBlock;
import org.apache.hadoop.io.erasurecode.ECBlockGroup;
import org.apache.hadoop.io.erasurecode.ErasureCodeConstants;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bandwidth-Optimal erasure decoder that decodes a block group.
 */
@InterfaceAudience.Private
public class BCCErasureDecoder extends ErasureDecoder {
    private RawErasureDecoder ccRawDecoder;
    private RawErasureEncoder ccRawEncoder;
    private RawErasureEncoder xorRawEncoder;

    static final Logger LOG = LoggerFactory.getLogger(BCCErasureDecoder.class);

    public BCCErasureDecoder(ErasureCoderOptions options) {
        super(options);
    }

    @Override
    protected ErasureCodingStep prepareDecodingStep(
            final ECBlockGroup blockGroup) {

        RawErasureDecoder rawDecoder;
        RawErasureEncoder rawEncoder;

        ECBlock[] inputBlocks = getInputBlocks(blockGroup);
        ECBlock[] outputBlocks = getOutputBlocks(blockGroup);

        rawDecoder = checkCreateCCRawDecoder();
        rawEncoder = checkCreateCCRawEncoder();

//        return new ErasureDecodingStep(inputBlocks,
//                getErasedIndexes(inputBlocks), outputBlocks, rawDecoder,
//                rawEncoder, this.getNumParityUnits(), this.numParityUnitsFinal);
        return new ErasureDecodingStep(inputBlocks,
                getErasedIndexes(inputBlocks), outputBlocks, rawDecoder);
    }

    private RawErasureDecoder checkCreateCCRawDecoder() {
        ErasureCoderOptions CCOptions = new ErasureCoderOptions(getOptions().getNumDataUnits(), this.numParityUnitsFinal);
        if (ccRawDecoder == null) {
            ccRawDecoder = CodecUtil.createRawDecoder(getConf(),
                    ErasureCodeConstants.CC_CODEC_NAME, CCOptions);
        }
        return ccRawDecoder;
    }

    private RawErasureEncoder checkCreateCCRawEncoder() {
        ErasureCoderOptions CCOptions = new ErasureCoderOptions(getOptions().getNumDataUnits(), numParityUnitsFinal);
        if (ccRawEncoder == null) {
            ccRawEncoder = CodecUtil.createRawEncoder(getConf(),
                    ErasureCodeConstants.CC_CODEC_NAME, CCOptions);
        }
        return ccRawEncoder;
    }

    private RawErasureEncoder checkCreateXorRawEncoder() {
        if (xorRawEncoder == null) {
            xorRawEncoder = CodecUtil.createRawEncoder(getConf(),
                    ErasureCodeConstants.XOR_CODEC_NAME, getOptions());
        }
        return xorRawEncoder;
    }

    @Override
    public boolean preferDirectBuffer() {
        return false;
    }

    @Override
    public void release() {
        if (ccRawDecoder != null) {
            ccRawDecoder.release();
        }
        if (xorRawEncoder != null) {
            xorRawEncoder.release();
        }
    }
}