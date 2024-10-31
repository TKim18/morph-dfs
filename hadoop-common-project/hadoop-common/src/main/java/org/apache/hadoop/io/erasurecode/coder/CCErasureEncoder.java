package org.apache.hadoop.io.erasurecode.coder;

import org.apache.hadoop.io.erasurecode.*;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;

public class CCErasureEncoder extends ErasureEncoder{
    private RawErasureEncoder rawEncoder;

    public CCErasureEncoder(ErasureCoderOptions options) {
        super(options);
    }

    @Override
    protected ErasureCodingStep prepareEncodingStep(final ECBlockGroup blockGroup) {

        RawErasureEncoder rawEncoder = checkCreateCCRawEncoder();

        ECBlock[] inputBlocks = getInputBlocks(blockGroup);

        return new ErasureEncodingStep(inputBlocks,
                getOutputBlocks(blockGroup), rawEncoder);
    }

    private RawErasureEncoder checkCreateCCRawEncoder() {
        if (rawEncoder == null) {
            // TODO: we should create the raw coder according to codec.
            rawEncoder = CodecUtil.createRawEncoder(getConf(),
                    ErasureCodeConstants.CC_CODEC_NAME, getOptions());
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
