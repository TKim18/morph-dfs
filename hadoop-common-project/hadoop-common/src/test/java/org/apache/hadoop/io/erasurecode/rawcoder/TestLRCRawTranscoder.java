package org.apache.hadoop.io.erasurecode.rawcoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.util.DumpUtil;
import org.junit.Test;

/**
 * Test base for raw CC -> LRC transcoder.
 */
public class TestLRCRawTranscoder {
    private int numDataUnits;
    private int numParityUnits;
    private int numNewDataUnits;
    private int numNewLocalUnits;
    private int numNewParityUnits;
    private int mergeFactor;
    private int blockSize;
    protected RawErasureTranscoder transcoder;
    protected RawErasureEncoder lrcRawEncoder;
    protected RawErasureEncoder ccRawEncoder3;
    protected static Random RAND = new Random();

    protected RawErasureEncoder ccRawEncoder;

    @Test
    public void testTranscode() throws IOException {
        numDataUnits = 6;
        numParityUnits = 3;
        numNewDataUnits = 12;
        numNewParityUnits = 2;
        mergeFactor = numNewDataUnits / numDataUnits;
        numNewLocalUnits = mergeFactor;
        blockSize = 8;

        ErasureCoderOptions ccGroupOptions = new ErasureCoderOptions(numDataUnits, numParityUnits);
        ccRawEncoder = prepCCEncoder(ccGroupOptions);
        ErasureCoderOptions lrcFinalGroupOptions = new ErasureCoderOptions(numNewDataUnits, numNewLocalUnits, numNewParityUnits);
        lrcRawEncoder = prepLRCEncoder(lrcFinalGroupOptions);

        // prepare first group in the file
        byte[][] dataInputs1 = prepareRandomizedInput(numDataUnits, blockSize);
        DumpUtil.dumpArrayOfBytes("dataInputs1", dataInputs1);
        byte[][] oldParities1 = new byte[numParityUnits][];
        for (int i = 0; i < numParityUnits; ++i) {
            oldParities1[i] = new byte[blockSize];
        }
        ByteArrayEncodingState baesGroup1 = new ByteArrayEncodingState(ccRawEncoder, dataInputs1, oldParities1);
        ccRawEncoder.doEncode(baesGroup1);
        DumpUtil.dumpArrayOfBytes("Encoding done: oldParities1", oldParities1);

        // prepare second group in the file
        byte[][] dataInputs2 = prepareRandomizedInput(numDataUnits, blockSize);
        DumpUtil.dumpArrayOfBytes("dataInputs2", dataInputs2);
        byte[][] oldParities2 = new byte[numParityUnits][];
        for (int i = 0; i < numParityUnits; ++i) {
            oldParities2[i] = new byte[blockSize];
        }
        ByteArrayEncodingState baesGroup2 = new ByteArrayEncodingState(ccRawEncoder, dataInputs2, oldParities2);
        ccRawEncoder.doEncode( baesGroup2);
        DumpUtil.dumpArrayOfBytes("Encoding done: oldParities2", oldParities2);


        // prepare (data and parity) inputs for transcoding using the groups created above.
        byte[][] allDataInputs = new byte[2*numDataUnits][];
        System.arraycopy(dataInputs1,0, allDataInputs,0, numDataUnits);
        System.arraycopy(dataInputs2,0, allDataInputs, numDataUnits, numDataUnits);
        DumpUtil.dumpArrayOfBytes("All data inputs", allDataInputs);
        byte[][] allParityInputs = new byte[2*numParityUnits][];
        System.arraycopy(oldParities1, 0, allParityInputs, 0, numParityUnits);
        System.arraycopy(oldParities2, 0, allParityInputs, numParityUnits, numParityUnits);
        DumpUtil.dumpArrayOfBytes("All old parity units", allParityInputs);

        // prepare parities for ground truth merged group
        byte[][] newParitiesViaEncoding = new byte[numNewLocalUnits + numNewParityUnits][];
        for (int i = 0; i < numNewLocalUnits + numNewParityUnits; ++i) {
            newParitiesViaEncoding[i] = new byte[blockSize];
        }
        ByteArrayEncodingState baesMergedGroundTruth = new ByteArrayEncodingState(lrcRawEncoder, allDataInputs, newParitiesViaEncoding);
        lrcRawEncoder.doEncode(baesMergedGroundTruth);

        // parities for merged CC group
        byte[][] newParitiesViaCCTranscode = doCCTranscoding(allParityInputs, numDataUnits, numNewDataUnits, numParityUnits);


        //Calculate new parities
        try {
            byte[][] newParitiesViaTranscoding = doTranscoding(allParityInputs, numDataUnits, numParityUnits, numNewDataUnits, numNewLocalUnits, numNewParityUnits);
            DumpUtil.dumpArrayOfBytes("Parities when data encoded", newParitiesViaEncoding);
            DumpUtil.dumpArrayOfBytes("CC Parities when data transcoded", newParitiesViaCCTranscode);
            DumpUtil.dumpArrayOfBytes("Parities when data transcoded", newParitiesViaTranscoding);
            System.out.println("Here Are the Parities When Transcoding");
            boolean result = Arrays.deepEquals(newParitiesViaEncoding, newParitiesViaTranscoding);
            System.out.println(result);
            if (!result) {
                throw new RuntimeException();
            }
            //boolean result = newParitiesViaEncoding.deepEquals(newParitiesViaTranscoding);
            //System.out.println(result);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Test
    public void testEncode() throws IOException {
        numDataUnits = 12;
        numParityUnits = 2;
        int numLocalUnits = 2;
        blockSize = 8;

        ErasureCoderOptions lrcGroupOptions = new ErasureCoderOptions(numDataUnits, numLocalUnits, numParityUnits);
        lrcRawEncoder = prepLRCEncoder(lrcGroupOptions);
        // for ground truth
        ErasureCoderOptions ccLocalGroupOptions = new ErasureCoderOptions(numDataUnits / numLocalUnits, 1);
        RawErasureEncoder ccLocalEncoder = prepCCEncoder(ccLocalGroupOptions);
        ErasureCoderOptions ccGlobalGroupOptions = new ErasureCoderOptions(numDataUnits, 1 + numParityUnits);
        RawErasureEncoder ccRawEncoder = prepCCEncoder(ccGlobalGroupOptions);

        // prepare first group in the file
        byte[][] dataInputs = prepareRandomizedInput(numDataUnits, blockSize);
        DumpUtil.dumpArrayOfBytes("dataInputs", dataInputs);

        // compute LRC parities
        byte[][] lrcParities = new byte[numLocalUnits + numParityUnits][];
        for (int i = 0; i < numLocalUnits + numParityUnits; ++i) {
            lrcParities[i] = new byte[blockSize];
        }
        ByteArrayEncodingState lrcBAES = new ByteArrayEncodingState(lrcRawEncoder, dataInputs, lrcParities);
        lrcRawEncoder.doEncode(lrcBAES);
        DumpUtil.dumpArrayOfBytes("Encoding done: LRC Parities", lrcParities);

        // ground truth
        // first check local parity units
        for (int i = 0; i < numLocalUnits; ++i) {
            byte[][] localDataInputs = new byte[numDataUnits / numLocalUnits][];
            System.arraycopy(dataInputs, i * numDataUnits / numLocalUnits, localDataInputs, 0, numDataUnits / numLocalUnits);
            byte[][] localParities = new byte[1][];
            localParities[0] = new byte[blockSize];
            ByteArrayEncodingState ccLocalBAES = new ByteArrayEncodingState(ccLocalEncoder, localDataInputs, localParities);
            ccLocalEncoder.doEncode(ccLocalBAES);
            DumpUtil.dumpArrayOfBytes("Encoding done: localParities" + i, localParities);
            boolean result = Arrays.deepEquals(localParities, new byte[][]{lrcParities[i]});
            if (!result) {
                throw new RuntimeException();
            }
        }
        // now check the global parities
        // 1+numParityUnits global parities because the first one is reserved to be used as local parity
        byte[][] globalParities = new byte[1+numParityUnits][];
        for (int i = 0; i < 1+numParityUnits; ++i) {
            globalParities[i] = new byte[blockSize];
        }
        ByteArrayEncodingState ccGlobalBAES = new ByteArrayEncodingState(ccRawEncoder, dataInputs, globalParities);
        ccRawEncoder.doEncode(ccGlobalBAES);
        DumpUtil.dumpArrayOfBytes("Encoding done: globalParities", globalParities);
        byte[][] globalParitiesFromLRC = new byte[numParityUnits][];
        System.arraycopy(lrcParities, numLocalUnits, globalParitiesFromLRC, 0, numParityUnits);
        byte[][] globlaParitiesFromCC = new byte[numParityUnits][];
        System.arraycopy(globalParities, 1, globlaParitiesFromCC, 0, numParityUnits);
        boolean result = Arrays.deepEquals(globlaParitiesFromCC, globalParitiesFromLRC);
        if (!result) {
            throw new RuntimeException();
        }


    }

    protected byte[][] doTranscoding(byte[][] parityInputs, int oldDataUnits, int oldParityUnits, int newDataUnits, int newLocalParityUnits, int newParityUnits) throws IOException {
        byte[][] outputs = new byte[newLocalParityUnits + newParityUnits][];
        for (int i = 0; i < newLocalParityUnits + newParityUnits; ++i) {
            outputs[i] = new byte[blockSize];
        }
        String codecName = "lrc";
        TranscoderOptions options = new TranscoderOptions(newDataUnits, newParityUnits, new ECSchema(codecName, oldDataUnits, oldParityUnits, newDataUnits, newParityUnits));
        transcoder = new LRCRawErasureTranscoder(options);
        int[] inputOffsets = new int[parityInputs.length];
        int[] outputOffsets = new int[outputs.length];
        ByteArrayTranscodingState batsMergedGroup = new ByteArrayTranscodingState(transcoder, blockSize, parityInputs, inputOffsets, outputs, outputOffsets);
        transcoder.doTranscode(batsMergedGroup);
        return batsMergedGroup.outputs;
    }

    protected  byte[][] doCCTranscoding(byte[][] parityInputs, int oldDataUnits, int newDataUnits, int newParityUnits) throws IOException {
        byte[][] outputs = new byte[newParityUnits][];
        int oldParityUnits = newParityUnits;
        for (int i = 0; i < newParityUnits; ++i) {
            outputs[i] = new byte[blockSize];
        }
        String codecName = "cc";
        TranscoderOptions options = new TranscoderOptions(newDataUnits, newParityUnits, new ECSchema(codecName, oldDataUnits, oldParityUnits));
        transcoder = new CCRawTranscoder(options);
        int[] inputOffsets = new int[parityInputs.length];
        int[] outputOffsets = new int[outputs.length];
        ByteArrayTranscodingState batsMergedGroup = new ByteArrayTranscodingState(transcoder, blockSize, parityInputs, inputOffsets, outputs, outputOffsets);
        transcoder.doTranscode(batsMergedGroup);
        return batsMergedGroup.outputs;
    }


    protected byte[][] prepareRandomizedInput(int inputSize, int blockSize){
        byte[][] inputs = new byte[inputSize][];
        for(int i = 0; i < inputSize; i++){
            inputs[i] = new byte[blockSize];
            RAND.nextBytes(inputs[i]);
            for (int j = 0; j < blockSize; ++j) {
                inputs[i][j] = 1;
            }
        }
        return inputs;
    }

    protected RawErasureEncoder prepCCEncoder(ErasureCoderOptions ccGroupOptions){
        return new CCRawEncoder(ccGroupOptions);
    }

    protected RawErasureEncoder prepLRCEncoder(ErasureCoderOptions lrcFinalGroupOptions){
        return new LRCRawEncoder(lrcFinalGroupOptions);
    }
}
