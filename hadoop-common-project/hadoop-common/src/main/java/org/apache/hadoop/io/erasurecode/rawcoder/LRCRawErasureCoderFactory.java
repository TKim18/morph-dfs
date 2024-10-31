package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ErasureCodeConstants;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class LRCRawErasureCoderFactory implements RawErasureCoderFactory{

    public static final String CODER_NAME = "lrc_java";
    static final Logger LOG = LoggerFactory.getLogger(LRCRawErasureCoderFactory.class);
    @Override
    public RawErasureEncoder createEncoder(ErasureCoderOptions coderOptions) { return new LRCRawEncoder(coderOptions); }

    @Override
    public RawErasureTranscoder createTranscoder(TranscoderOptions transcoderOptions) {
        return new LRCRawErasureTranscoder(transcoderOptions);
    }

    @Override
    public RawErasureDecoder createDecoder(ErasureCoderOptions coderOptions) { return new LRCRawDecoder(coderOptions); }

    @Override
    public String getCoderName() { return CODER_NAME; }

    @Override
    public String getCodecName() { return ErasureCodeConstants.LRC_CODEC_NAME; }
}
