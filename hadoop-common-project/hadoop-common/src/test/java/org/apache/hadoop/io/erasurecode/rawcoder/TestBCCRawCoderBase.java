package org.apache.hadoop.io.erasurecode.rawcoder;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.erasurecode.ECChunk;
import org.apache.hadoop.io.erasurecode.rawcoder.util.GF256;
import org.junit.Test;
import org.apache.hadoop.io.erasurecode.TestCoderBase;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.util.RSUtil;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @TODO: Current BCC implementation doesn't cover all corner cases yet.
 * 1. Add support for ByteBuffers.
 * 2. Add support for irregular chunk sizes.
 *
 *  To run the tests:
 *  1. Works for testCoding(false) in TestRawCoderBase::testCodingDoMixed().  Comment out testCoding(true).
 *  2. Set usingSlicedBuffer to false.  performTestCoding(baseChunkSize, false, false, false, false) in TestRawCoderBase::testCoding().
 *  3. Comment out tests with irregular chunks sizes in TestRawCoderBase::testCoding().
 *  4. Comment out startBufferWithZero = ! startBufferWithZero;  in TestCoderBase::allocateOutputBuffer().
 */

public class TestBCCRawCoderBase extends TestRawCoderBase {

//    @Test
//    public void testCoding_6x3_erasing_all_d() {
//        prepare(null, 6, 1, 12, 2, new int[]{0}, new int[0], true);
//        testCodingDoMixAndTwice();
//    }

    @Test
    public void testCoding_6x1_erasing_d0() {
        prepare(null, 6, 1, 12, 2, new int[] {0}, new int[0], true);
        testCodingDoMixAndTwice();
    }
}


