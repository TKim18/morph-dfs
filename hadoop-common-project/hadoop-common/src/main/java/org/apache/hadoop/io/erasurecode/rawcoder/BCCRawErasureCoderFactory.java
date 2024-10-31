package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ErasureCodeConstants;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class BCCRawErasureCoderFactory implements RawErasureCoderFactory{

    public static final String CODER_NAME = "bcc_java";
    static final Logger LOG = LoggerFactory.getLogger(BCCRawErasureCoderFactory.class);
    @Override
    public RawErasureEncoder createEncoder(ErasureCoderOptions coderOptions) {
        LOG.info("[BWOCC] BCCRawErasureEncoder() with ErasureCoderOptions with extra bits {} {}", coderOptions.getNumDataUnitsFinal(), coderOptions.getNumParityUnitsFinal());
        return new BCCRawEncoder(coderOptions);
    }

    @Override
    public RawErasureTranscoder createTranscoder(TranscoderOptions transcoderOptions) {
        return new BCCRawTranscoder(transcoderOptions);
    }

    @Override
    public RawErasureDecoder createDecoder(ErasureCoderOptions coderOptions) {return new BCCRawDecoder(coderOptions);}

    @Override
    public String getCoderName() {
        return CODER_NAME;
    }

    @Override
    public String getCodecName() {
        return ErasureCodeConstants.BCC_CODEC_NAME;
    }
}
