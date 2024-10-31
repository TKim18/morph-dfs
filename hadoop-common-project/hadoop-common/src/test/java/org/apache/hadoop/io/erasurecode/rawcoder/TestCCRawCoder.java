package org.apache.hadoop.io.erasurecode.rawcoder;

import org.junit.Before;

public class TestCCRawCoder extends TestRSRawCoderBase{

    @Before
    public void setup() {
        this.encoderFactoryClass = CCRawErasureCoderFactory.class;
        this.decoderFactoryClass = CCRawErasureCoderFactory.class;
        setAllowDump(false);
    }
}
