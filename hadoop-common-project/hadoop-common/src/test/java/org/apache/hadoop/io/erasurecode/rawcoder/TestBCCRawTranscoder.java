package org.apache.hadoop.io.erasurecode.rawcoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.util.DumpUtil;
import org.junit.Test;

public class TestBCCRawTranscoder {
    private int numDataUnits;
    private int numParityUnits;
    private int numNewDataUnits;
    private int numNewParityUnits;
    private int numCellsInBlock;
    private int blockSize;
    protected RawErasureTranscoder transcoder;
    protected RawErasureEncoder ccRawEncoder;
    protected RawErasureEncoder ccRawEncoder3;
    protected static Random RAND = new Random();

    protected RawErasureEncoder bccRawEncoder;

    @Test
    public void testTranscode() throws IOException {
        numDataUnits = 6;
        numParityUnits = 3;
        numNewDataUnits = 12;
        numNewParityUnits = 4;
        numCellsInBlock = 1;
        blockSize = 8;

        ErasureCoderOptions bccGroup1Options = new ErasureCoderOptions(numDataUnits, numParityUnits, numNewDataUnits, numNewParityUnits, numCellsInBlock);
        prepEncoderBCCGroup(bccGroup1Options);
        ErasureCoderOptions ccFinalGroupOptions = new ErasureCoderOptions(numNewDataUnits, numNewParityUnits);
        prepEncoderCCFinal(ccFinalGroupOptions);

        // prepare first group in the file
        byte[][] dataInputs1 = prepareRandomizedInput(numDataUnits, blockSize);
        DumpUtil.dumpArrayOfBytes("dataInputs1", dataInputs1);
        byte[][] oldParities1 = new byte[numParityUnits][];
        for (int i = 0; i < numParityUnits; ++i) {
            oldParities1[i] = new byte[blockSize];
        }
        ByteArrayEncodingState baesGroup1 = new ByteArrayEncodingState(bccRawEncoder, dataInputs1, oldParities1);
        bccRawEncoder.doEncode(baesGroup1);
        DumpUtil.dumpArrayOfBytes("Encoding done: oldParities1", oldParities1);

        // prepare second group in the file
        byte[][] dataInputs2 = prepareRandomizedInput(numDataUnits, blockSize);
        DumpUtil.dumpArrayOfBytes("dataInputs2", dataInputs2);
        byte[][] oldParities2 = new byte[numParityUnits][];
        for (int i = 0; i < numParityUnits; ++i) {
            oldParities2[i] = new byte[blockSize];
        }
        ByteArrayEncodingState baesGroup2 = new ByteArrayEncodingState(bccRawEncoder, dataInputs2, oldParities2);
        bccRawEncoder.doEncode( baesGroup2);
        DumpUtil.dumpArrayOfBytes("Encoding done: oldParities2", oldParities2);


        // prepare (data and parity) inputs for transcoding using the groups created above.
        byte[][] allDataInputs = new byte[2*numDataUnits][];
        System.arraycopy(dataInputs1,0, allDataInputs,0, numDataUnits);
        System.arraycopy(dataInputs2,0, allDataInputs, numDataUnits, numDataUnits);
        DumpUtil.dumpArrayOfBytes("All data inputs", allDataInputs);
        byte[][] allParityInputs = new byte[2*numParityUnits][];
        System.arraycopy(oldParities1, 0, allParityInputs, 0, numParityUnits);
        System.arraycopy(oldParities2, 0, allParityInputs, numParityUnits, numParityUnits);
        DumpUtil.dumpArrayOfBytes("all Old parity units", allParityInputs);
        System.out.println("Here is All Old parities");

        // prepare parities for ground truth merged group
        byte[][] newParitiesViaEncoding = new byte[numNewParityUnits][];
        for (int i = 0; i < numNewParityUnits; ++i) {
            newParitiesViaEncoding[i] = new byte[blockSize];
        }
        ByteArrayEncodingState baesMergedGroundTruth = new ByteArrayEncodingState(ccRawEncoder, allDataInputs, newParitiesViaEncoding);
        ccRawEncoder.doEncode(baesMergedGroundTruth);

        //Calculate new parities
        try {
            System.out.println("Transcoding Should Be Called Here");
            byte[][] newParitiesViaTranscoding = doTranscoding(allDataInputs, allParityInputs, numDataUnits, numParityUnits, numNewDataUnits, numNewParityUnits);
            DumpUtil.dumpArrayOfBytes("Parities when data encoded", newParitiesViaEncoding);
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

    protected byte[][] doTranscoding(byte[][] dataInputs, byte[][] parityInputs, int oldDataUnits, int oldParityUnits, int newDataUnits, int newParityUnits) throws IOException {
        byte[][] outputs = new byte[newParityUnits][];
        for (int i = 0; i < newParityUnits; ++i) {
            outputs[i] = new byte[blockSize];
        }
        String codecName = "bcc";
        TranscoderOptions options = new TranscoderOptions(newDataUnits, newParityUnits, new ECSchema(codecName, oldDataUnits, oldParityUnits, newDataUnits, newParityUnits));
        transcoder = new BCCRawTranscoder(options);
        byte[][] allInputs = new byte[2*oldDataUnits + 2*oldParityUnits][];
        int subBlockSize = blockSize / numNewParityUnits;
        int extraParities = numNewParityUnits - numParityUnits;
        int offset = numParityUnits * subBlockSize;
        for (int i = 0; i < allInputs.length; ++i) {
            if (i < 2 * oldDataUnits) {
                allInputs[i] = new byte[extraParities * subBlockSize];
            }
            else {
                allInputs[i] = new byte[blockSize];
            }
        }
        for (int i = 0; i < 2 * oldDataUnits; ++i) {
            System.arraycopy(dataInputs[i], offset, allInputs[i], 0, extraParities * subBlockSize);
        }
        System.arraycopy(parityInputs, 0, allInputs, 2 * oldDataUnits, 2 * oldParityUnits);
        int[] inputOffsets = new int[allInputs.length];
        int[] outputOffsets = new int[outputs.length];
        ByteArrayTranscodingState batsMergedGroup = new ByteArrayTranscodingState(transcoder, blockSize, allInputs, inputOffsets, outputs, outputOffsets);
        transcoder.doTranscode(batsMergedGroup);
        return batsMergedGroup.outputs;
    }


    protected byte[][] prepareRandomizedInput(int inputSize, int blockSize){
        byte[][] inputs = new byte[inputSize][];
        for(int i = 0; i < inputSize; i++){
            inputs[i] = new byte[blockSize];
            RAND.nextBytes(inputs[i]);
//            for (int j = 0; j < blockSize; ++j) {
//                inputs[i][j] = 1;
//            }
        }
        return inputs;
    }

    protected void prepEncoderBCCGroup(ErasureCoderOptions bccGroup1Options){
        bccRawEncoder = new BCCRawEncoder(bccGroup1Options);
    }

    protected void prepEncoderCCFinal(ErasureCoderOptions ccFinalGroupOptions){
        ccRawEncoder = new CCRawEncoder(ccFinalGroupOptions);
    }
}
