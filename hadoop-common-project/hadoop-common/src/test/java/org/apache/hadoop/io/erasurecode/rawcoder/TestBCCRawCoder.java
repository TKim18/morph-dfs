package org.apache.hadoop.io.erasurecode.rawcoder;

import org.junit.Before;

public class TestBCCRawCoder extends TestBCCRawCoderBase{

    @Before
    public void setup() {
        this.encoderFactoryClass = BCCRawErasureCoderFactory.class;
        this.decoderFactoryClass = BCCRawErasureCoderFactory.class;
        setAllowDump(true);
    }
}
