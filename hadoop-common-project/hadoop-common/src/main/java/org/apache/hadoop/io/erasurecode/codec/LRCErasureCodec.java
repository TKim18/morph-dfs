package org.apache.hadoop.io.erasurecode.codec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.erasurecode.ErasureCodecOptions;
import org.apache.hadoop.io.erasurecode.coder.*;

public class LRCErasureCodec extends ErasureCodec {
    public LRCErasureCodec(Configuration conf, ErasureCodecOptions options) {
        super(conf, options);
    }

    @Override
    public ErasureEncoder createEncoder() {
        return new LRCErasureEncoder(getCoderOptions());
    }

    @Override
    public ErasureDecoder createDecoder() {
        return new LRCErasureDecoder(getCoderOptions());
    }
}
