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

public class TestCCRawCoderBase extends TestRawCoderBase {

    @Test
    public void testCCVandermonde(){
        byte[] VMatrix = new byte[(6 + 3) * 6];
        RSUtil.genRSMatrix(VMatrix, 9, 6);
        System.out.println(Arrays.toString(VMatrix));
    }

    @Test
    public void testGFMul(){
        System.out.println(GF256.gfMul((byte) 1, (byte) 2));
    }

    @Test
    public void testCoding_6x3_erasing_all_d() {
        prepare(null, 6, 3, new int[]{0, 1, 2}, new int[0], true);
        testCodingDoMixAndTwice();
    }
    @Test
    public void testCoding_6x3_erasing_d0_d2() {
        prepare(null, 6, 3, new int[] {0, 2}, new int[]{});
        testCodingDoMixAndTwice();
    }

    @Test
    public void testCoding_6x3_erasing_d0() {
        prepare(null, 6, 3, new int[]{0}, new int[0]);
        testCodingDoMixAndTwice();
    }

    @Test
    public void testCoding_6x3_erasing_d2() {
        prepare(null, 6, 3, new int[]{2}, new int[]{});
        testCodingDoMixAndTwice();
    }

    @Test
    public void testCoding_6x3_erasing_d0_p0() {
        prepare(null, 6, 3, new int[]{0}, new int[]{0});
        testCodingDoMixAndTwice();
    }

    @Test
    public void testCoding_6x3_erasing_all_p() {
        prepare(null, 6, 3, new int[0], new int[]{0, 1, 2});
        testCodingDoMixAndTwice();
    }

    @Test
    public void testCoding_6x3_erasing_p0() {
        prepare(null, 6, 3, new int[0], new int[]{0});
        testCodingDoMixAndTwice();
    }

    @Test
    public void testCoding_6x3_erasing_p2() {
        prepare(null, 6, 3, new int[0], new int[]{2});
        testCodingDoMixAndTwice();
    }

    @Test
    public void testCoding_6x3_erasure_p0_p2() {
        prepare(null, 6, 3, new int[0], new int[]{0, 2});
        testCodingDoMixAndTwice();
    }

    @Test
    public void testCoding_6x3_erasing_d0_p0_p1() {
        prepare(null, 6, 3, new int[]{0}, new int[]{0, 1});
        testCodingDoMixAndTwice();
    }

    @Test
    public void testCoding_6x3_erasing_d0_d2_p2() {
        prepare(null, 6, 3, new int[]{0, 2}, new int[]{2});
        testCodingDoMixAndTwice();
    }

    @Test
    public void testCodingNegative_6x3_erasing_d2_d4() {
        prepare(null, 6, 3, new int[]{2, 4}, new int[0]);
        testCodingDoMixAndTwice();
    }
}


