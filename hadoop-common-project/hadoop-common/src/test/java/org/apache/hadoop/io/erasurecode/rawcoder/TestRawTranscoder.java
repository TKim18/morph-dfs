package org.apache.hadoop.io.erasurecode.rawcoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.junit.Test;

public class TestRawTranscoder {
//    protected RawTranscoder transcoder;
//    protected RawErasureEncoder ccRawEncoder;
//
//    protected RawErasureEncoder ccRawEncoder2;
//
//    protected RawErasureEncoder ccRawEncoder3;
//    protected static Random RAND = new Random();
//
//    @Test
//    public void testTranscode() throws IOException {
//        prepEncoderSixOfNine();
//        prepEncoderTwelveOfFifteen();
//        ByteBuffer[] dataInputs1 = prepareRandomizedInput(6, 8);
//        System.out.println("Here is DataInputs1");
//        for(int i=0; i < 6; i++){
//            dataInputs1[i].position(0);
//            for (int j = 0; j < 8; j++) {
//                System.out.print(dataInputs1[i].get() + " ");
//            }
//            System.out.println(" ");
//        }
//        for (int i = 0; i < 6; i++) {
//            dataInputs1[i].position(0);
//        }
//        ByteBuffer[] oldParities1 = new ByteBuffer[]{ByteBuffer.allocateDirect(8),ByteBuffer.allocateDirect(8),
//                ByteBuffer.allocateDirect(8)};
//
//        for (int i = 0; i < 3; i++) {
//            oldParities1[i].position(0);
//        }
//        System.out.println("Encoding Should be Called Here");
//       ccRawEncoder.doEncode(new ByteBufferEncodingState(ccRawEncoder, dataInputs1, oldParities1));
//
//        System.out.println("Here is oldParities1");
//        for(int i=0; i < 3; i++){
//            oldParities1[i].position(0);
//            for (int j = 0; j < 8; j++) {
//                System.out.print(oldParities1[i].get() + " ");
//            }
//            System.out.println(" ");
//        }
//
//        ByteBuffer[] dataInputs2 = prepareRandomizedInput(6, 8);
//        System.out.println("Here is dataInputs2");
//        for(int i=0; i < 6; i++){
//            dataInputs2[i].position(0);
//            for (int j = 0; j < 8; j++) {
//                System.out.print(dataInputs2[i].get() + " ");
//            }
//            System.out.println(" ");
//        }
//        for (int i = 0; i < 6; i++) {
//            dataInputs2[i].position(0);
//        }
//        ByteBuffer[] oldParities2 = new ByteBuffer[]{ByteBuffer.allocateDirect(8),ByteBuffer.allocateDirect(8),
//                ByteBuffer.allocateDirect(8)};
//
//
//        for (int i = 0; i < 3; i++) {
//            oldParities2[i].position(0);
//        }
//        ccRawEncoder.doEncode(new ByteBufferEncodingState(ccRawEncoder, dataInputs2, oldParities2));
//        System.out.println("Here is oldParities2");
//        for(int i=0; i < 3; i++){
//            oldParities2[i].position(0);
//            for (int j = 0; j < 8; j++) {
//                System.out.print(oldParities2[i].get() + " ");
//            }
//            System.out.println(" ");
//        }
//
//
//
//        ByteBuffer[] allDataInputs = new ByteBuffer[6+6];
//        System.arraycopy(dataInputs1,0,allDataInputs,0,6);
//        System.arraycopy(dataInputs2,0,allDataInputs,6,6);
////        System.out.println("Here is All Data inputs");
////        for(int i=0; i < 12; i++){
////            allDataInputs[i].position(0);
////            for (int j = 0; j < 8; j++) {
////                System.out.print(allDataInputs[i].get() + " ");
////            }
////            System.out.println(" ");
////        }
//
//
//        ByteBuffer[] allOldParities = new ByteBuffer[3+3];
//        System.arraycopy(oldParities1, 0, allOldParities, 0, 3);
//        System.arraycopy(oldParities2, 0, allOldParities, 3, 3);
//
//
//
//        System.out.println("Here is All Old parities");
//        for(int i=0; i < 6; i++){
//            allOldParities[i].position(0);
//            for (int j = 0; j < 8; j++) {
//                System.out.print(allOldParities[i].get() + " ");
//            }
//            System.out.println(" ");
//        }
//        for (int i = 0; i < 6; i++) {
//            allOldParities[i].position(0);
//        }
//
//        ByteBuffer[] newParitiesViaEncoding = new ByteBuffer[]{ByteBuffer.allocateDirect(8),ByteBuffer.allocateDirect(8),
//                ByteBuffer.allocateDirect(8)};
//        ccRawEncoder2.doEncode(new ByteBufferEncodingState(ccRawEncoder2, allDataInputs, newParitiesViaEncoding));
//
//        //Calculate new parities
//        try {
//            System.out.println("Transcoding Should Be Called Here");
//            ByteBuffer[] newParitiesViaTranscoding = doTranscoding(allOldParities,12,6,3);
//            System.out.println("Here Are the Parities WHen Encoding Data");
//            for(int i=0; i < 3; i++){
//                newParitiesViaEncoding[i].position(0);
//                for (int j = 0; j < 8; j++) {
//                    //System.out.print(newParitiesViaEncoding[i].get() + " ");
//                    System.out.printf("%2x ", newParitiesViaEncoding[i].get());
//                }
//                System.out.println(" ");
//            }
//            System.out.println("Here Are the Parities When Transcoding");
//            for(int i=0; i < 3; i++){
//                newParitiesViaTranscoding[i].position(0);
//                for (int j = 0; j < 8; j++) {
//                    //System.out.print(newParitiesViaTranscoding[i].get() + " ");
//                    System.out.printf("%2x ", newParitiesViaTranscoding[i].get());
//                }
//                System.out.println(" ");
//            }
//            boolean result = Arrays.deepEquals(newParitiesViaEncoding, newParitiesViaTranscoding);
//            System.out.println(result);
//            //boolean result = newParitiesViaEncoding.deepEquals(newParitiesViaTranscoding);
//            //System.out.println(result);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//
//    }
//
//    @Test
//    public void testTranscodeThreeMerge() throws IOException {
//        prepEncoderSixOfNine();
//        prepEncoderEighteenOfTwentyOne();
//        ByteBuffer[] dataInputs1 = prepareRandomizedInput(6, 8);
//        System.out.println("Here is DataInputs1");
//        for(int i=0; i < 6; i++){
//            dataInputs1[i].position(0);
//            for (int j = 0; j < 8; j++) {
//                System.out.print(dataInputs1[i].get() + " ");
//            }
//            System.out.println(" ");
//        }
//        for (int i = 0; i < 6; i++) {
//            dataInputs1[i].position(0);
//        }
//        ByteBuffer[] oldParities1 = new ByteBuffer[]{ByteBuffer.allocateDirect(8),ByteBuffer.allocateDirect(8),
//                ByteBuffer.allocateDirect(8)};
//
//        for (int i = 0; i < 3; i++) {
//            oldParities1[i].position(0);
//        }
//        System.out.println("Encoding Should be Called Here");
//        ccRawEncoder.doEncode(new ByteBufferEncodingState(ccRawEncoder, dataInputs1, oldParities1));
//
//        System.out.println("Here is oldParities1");
//        for(int i=0; i < 3; i++){
//            oldParities1[i].position(0);
//            for (int j = 0; j < 8; j++) {
//                System.out.print(oldParities1[i].get() + " ");
//            }
//            System.out.println(" ");
//        }
//
//        ByteBuffer[] dataInputs2 = prepareRandomizedInput(6, 8);
//        System.out.println("Here is dataInputs2");
//        for(int i=0; i < 6; i++){
//            dataInputs2[i].position(0);
//            for (int j = 0; j < 8; j++) {
//                System.out.print(dataInputs2[i].get() + " ");
//            }
//            System.out.println(" ");
//        }
//        for (int i = 0; i < 6; i++) {
//            dataInputs2[i].position(0);
//        }
//        ByteBuffer[] oldParities2 = new ByteBuffer[]{ByteBuffer.allocateDirect(8),ByteBuffer.allocateDirect(8),
//                ByteBuffer.allocateDirect(8)};
//
//
//        for (int i = 0; i < 3; i++) {
//            oldParities2[i].position(0);
//        }
//
//        ccRawEncoder.doEncode(new ByteBufferEncodingState(ccRawEncoder, dataInputs2, oldParities2));
//        System.out.println("Here is oldParities2");
//        for(int i=0; i < 3; i++){
//            oldParities2[i].position(0);
//            for (int j = 0; j < 8; j++) {
//                System.out.print(oldParities2[i].get() + " ");
//            }
//            System.out.println(" ");
//        }
//
//        ByteBuffer[] dataInputs3 = prepareRandomizedInput(6, 8);
//        System.out.println("Here is dataInputs3");
//        for(int i=0; i < 6; i++){
//            dataInputs3[i].position(0);
//            for (int j = 0; j < 8; j++) {
//                System.out.print(dataInputs3[i].get() + " ");
//            }
//            System.out.println(" ");
//        }
//        for (int i = 0; i < 6; i++) {
//            dataInputs3[i].position(0);
//        }
//        ByteBuffer[] oldParities3 = new ByteBuffer[]{ByteBuffer.allocateDirect(8),ByteBuffer.allocateDirect(8),
//                ByteBuffer.allocateDirect(8)};
//
//
//        for (int i = 0; i < 3; i++) {
//            oldParities3[i].position(0);
//        }
//
//        ccRawEncoder.doEncode(new ByteBufferEncodingState(ccRawEncoder, dataInputs3, oldParities3));
//        System.out.println("Here is oldParities3");
//        for(int i=0; i < 3; i++){
//            oldParities2[i].position(0);
//            for (int j = 0; j < 8; j++) {
//                System.out.print(oldParities3[i].get() + " ");
//            }
//            System.out.println(" ");
//        }
//
//        ByteBuffer[] allDataInputs = new ByteBuffer[6+6+6];
//        System.arraycopy(dataInputs1,0,allDataInputs,0,6);
//        System.arraycopy(dataInputs2,0,allDataInputs,6,6);
//        System.arraycopy(dataInputs3,0,allDataInputs,12,6);
////        System.out.println("Here is All Data inputs");
////        for(int i=0; i < 12; i++){
////            allDataInputs[i].position(0);
////            for (int j = 0; j < 8; j++) {
////                System.out.print(allDataInputs[i].get() + " ");
////            }
////            System.out.println(" ");
////        }
//
//
//        ByteBuffer[] allOldParities = new ByteBuffer[3+3+3];
//        System.arraycopy(oldParities1, 0, allOldParities, 0, 3);
//        System.arraycopy(oldParities2, 0, allOldParities, 3, 3);
//        System.arraycopy(oldParities3, 0, allOldParities, 6, 3);
//
//
//
//        System.out.println("Here is All Old parities");
//        for(int i=0; i < 9; i++){
//            allOldParities[i].position(0);
//            for (int j = 0; j < 8; j++) {
//                System.out.print(allOldParities[i].get() + " ");
//            }
//            System.out.println(" ");
//        }
//        for (int i = 0; i < 9; i++) {
//            allOldParities[i].position(0);
//        }
//
//        ByteBuffer[] newParitiesViaEncoding = new ByteBuffer[]{ByteBuffer.allocateDirect(8),ByteBuffer.allocateDirect(8),
//                ByteBuffer.allocateDirect(8)};
//        ccRawEncoder3.doEncode(new ByteBufferEncodingState(ccRawEncoder3, allDataInputs, newParitiesViaEncoding));
//
//        //Calculate new parities
//        try {
//            System.out.println("Transcoding Should Be Called Here");
//            ByteBuffer[] newParitiesViaTranscoding = doTranscoding(allOldParities,18,6,3);
//            System.out.println("Here Are the Parities WHen Encoding Data");
//            for(int i=0; i < 3; i++){
//                newParitiesViaEncoding[i].position(0);
//                for (int j = 0; j < 8; j++) {
//                    //System.out.print(newParitiesViaEncoding[i].get() + " ");
//                    System.out.printf("%2x ", newParitiesViaEncoding[i].get());
//                }
//                System.out.println(" ");
//            }
//            System.out.println("Here Are the Parities When Transcoding");
//            for(int i=0; i < 3; i++){
//                newParitiesViaTranscoding[i].position(0);
//                for (int j = 0; j < 8; j++) {
//                    //System.out.print(newParitiesViaTranscoding[i].get() + " ");
//                    System.out.printf("%2x ", newParitiesViaTranscoding[i].get());
//                }
//                System.out.println(" ");
//            }
//            boolean result = Arrays.deepEquals(newParitiesViaEncoding, newParitiesViaTranscoding);
//            System.out.println(result);
//            //boolean result = newParitiesViaEncoding.deepEquals(newParitiesViaTranscoding);
//            //System.out.println(result);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//
//    }
//
//
//
//    public void testGFTables(){
//
//    }
//    @Test
//    public void testRandomBuffer(){
//        System.out.println(prepareRandomizedBuffer(8));
//    }
//
//    protected ByteBuffer[] doTranscoding(ByteBuffer[] allOldParities, int newDataNum, int oldDataNum, int newParityNum) throws IOException {
//        //ByteBuffer[] inputs = prepareRandomizedInput(totalOldParities, bufferSize);
//        ByteBuffer[] outputs = new ByteBuffer[]{ByteBuffer.allocateDirect(8),ByteBuffer.allocateDirect(8),
//                ByteBuffer.allocateDirect(8)};
//        prepTranscoder(newDataNum, oldDataNum, newParityNum);
//        transcoder.transcode(allOldParities, outputs);
//        return outputs;
//    }
//
//    protected ByteBuffer[] prepareRandomizedInput(int inputSize, int bufferSize){
//        ByteBuffer[] inputs = new ByteBuffer[inputSize];
//        for(int i = 0; i < inputSize; i++){
//            inputs[i] = prepareRandomizedBuffer(bufferSize);
//        }
//
//        return inputs;
//    }
//
//    protected ByteBuffer prepareRandomizedBuffer(int bufferSize){
//        ByteBuffer randomizedBuffer = ByteBuffer.allocateDirect(bufferSize);
//        byte[] randomData = new byte[bufferSize];
//        RAND.nextBytes(randomData);
//        randomizedBuffer.put(randomData);
//
//        return randomizedBuffer;
//    }
//
//    protected void prepTranscoder(int newDataNum, int newParityNum, ECSchema oldSchema){
//        TranscoderOptions options = new TranscoderOptions(newDataNum, newParityNum, oldSchema);
//        //Should be raw transcoder at some point but no factory has been set up yet
//        transcoder = new CCRawTranscoder(options);
//
//    }
//
//    protected void prepEncoderSixOfNine(){
//        ErasureCoderOptions sixOfNineOption = new ErasureCoderOptions(6,3);
//        ccRawEncoder = new CCRawEncoder(sixOfNineOption);
//    }
//    protected void prepEncoderTwelveOfFifteen(){
//        ErasureCoderOptions twelveFifOption = new ErasureCoderOptions(12,3);
//        ccRawEncoder2 = new CCRawEncoder(twelveFifOption);
//    }
//
//    protected void prepEncoderEighteenOfTwentyOne(){
//        ErasureCoderOptions eighteenTwentyOneOption = new ErasureCoderOptions(18,3);
//        ccRawEncoder3 = new CCRawEncoder(eighteenTwentyOneOption);
//    }
//
//    protected void testTranscodeMatrix(){
//        System.out.println();
//    }






}
