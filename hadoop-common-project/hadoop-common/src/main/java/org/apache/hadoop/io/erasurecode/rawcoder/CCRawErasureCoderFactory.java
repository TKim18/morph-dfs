package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ErasureCodeConstants;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
@InterfaceAudience.Private
public class CCRawErasureCoderFactory implements RawErasureCoderFactory{

    public static final String CODER_NAME = "cc_java";
    @Override
    public RawErasureEncoder createEncoder(ErasureCoderOptions coderOptions) {
        return new CCRawEncoder(coderOptions);
    }

    @Override
    public RawErasureTranscoder createTranscoder(TranscoderOptions transcoderOptions) {
        return new CCRawTranscoder(transcoderOptions);
    }

    @Override
    public RawErasureDecoder createDecoder(ErasureCoderOptions coderOptions) {return new CCRawDecoder(coderOptions);}

    @Override
    public String getCoderName() {
        return CODER_NAME;
    }

    @Override
    public String getCodecName() {
        return ErasureCodeConstants.CC_CODEC_NAME;
    }
}
