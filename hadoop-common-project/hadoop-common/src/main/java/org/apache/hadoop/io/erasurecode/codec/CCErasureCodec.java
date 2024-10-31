package org.apache.hadoop.io.erasurecode.codec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.erasurecode.ErasureCodecOptions;
import org.apache.hadoop.io.erasurecode.coder.*;

public class CCErasureCodec extends ErasureCodec {
    public CCErasureCodec(Configuration conf, ErasureCodecOptions options) {
        super(conf, options);
    }

    @Override
    public ErasureEncoder createEncoder() {
        return new CCErasureEncoder(getCoderOptions());
    }

    @Override
    public ErasureDecoder createDecoder() {
        return new RSErasureDecoder(getCoderOptions());
    }
}
