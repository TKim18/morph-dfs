package org.apache.hadoop.io.erasurecode.coder;

import org.apache.hadoop.io.erasurecode.*;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;

public class LRCErasureEncoder extends ErasureEncoder {
    private RawErasureEncoder rawEncoder;

    public LRCErasureEncoder(ErasureCoderOptions options) {
        super(options);
    }

    @Override
    protected ErasureCodingStep prepareEncodingStep(final ECBlockGroup blockGroup) {

        RawErasureEncoder rawEncoder = checkCreateLRCRawEncoder();

        ECBlock[] inputBlocks = getInputBlocks(blockGroup);

        return new ErasureEncodingStep(inputBlocks,
                getOutputBlocks(blockGroup), rawEncoder);
    }

    private RawErasureEncoder checkCreateLRCRawEncoder() {
        if (rawEncoder == null) {
            rawEncoder = CodecUtil.createRawEncoder(getConf(),
                    ErasureCodeConstants.LRC_CODEC_NAME, getOptions());
        }
        return rawEncoder;
    }

    @Override
    public void release() {
        if (rawEncoder != null) {
            rawEncoder.release();
        }
    }

    @Override
    public boolean preferDirectBuffer() {
        return false;
    }

}
