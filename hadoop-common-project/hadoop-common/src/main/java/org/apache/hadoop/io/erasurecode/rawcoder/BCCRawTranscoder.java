package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.util.RSUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class BCCRawTranscoder extends RawErasureTranscoder {

    static final Logger LOG = LoggerFactory.getLogger(BCCRawTranscoder.class);

    private final byte[] transcodeMatrix;
    private final byte[] gfTables;
    private int subPacketSize;
    private CCRawEncoder encoderForStripesWOPiggyback;


    public BCCRawTranscoder(TranscoderOptions options){
        super(options);
        //We're going to need a parity to field size check, I think
        // LOG.info("[BWOCC]-t BCCRawTranscoder().  mergeFactor = {};  newParityNum = {}", options.getMergeFactor(), options.getNewParityNum());
        assert (options.getNewParityNum() == options.getOldSchema().getNumParityUnitsFinal());

        transcodeMatrix = new byte[options.getMergeFactor()*options.getNewParityNum()*options.getNewParityNum()];
        RSUtil.genCCTranscodingMatrix(transcodeMatrix, options.getMergeFactor(), options.getNewParityNum(), options.getOldDataNum());

        gfTables = new byte[options.getMergeFactor() * options.getNewParityNum() * options.getNewParityNum() * 32];
        RSUtil.initTables(options.getMergeFactor() * options.getNewParityNum(), options.getNewParityNum(),
                transcodeMatrix, 0, gfTables);


        subPacketSize = options.getOldSchema().getNumParityUnitsFinal();
        ErasureCoderOptions coderOptionsForStripesWOPiggyback = new ErasureCoderOptions(options.getOldDataNum(), options.getNewParityNum());
        encoderForStripesWOPiggyback = new CCRawEncoder(coderOptionsForStripesWOPiggyback);
    }

    protected void doTranscode(ByteBufferTranscodingState transcodingState){
        CoderUtil.resetOutputBuffers(transcodingState.outputs,
                transcodingState.transcodeLength);
        RSUtil.encodeData(gfTables, transcodingState.inputs, transcodingState.outputs);
    }

    protected void doTranscode(ByteArrayTranscodingState transcodingState) throws IOException {
        // LOG.info("[BWOCC]-t BCC.doTranscode() with ByteArrayTranscodingState.  inputs={}x{}, subPacketSize = {}", transcodingState.inputs.length, transcodingState.inputs[0].length, subPacketSize);
//        CoderUtil.resetOutputBuffers(transcodingState.outputs, transcodingState.outputOffsets,
//                transcodingState.transcodeLength);
//
//
//
//        byte[][] onlyDataInputs = new byte[2][];
//        System.arraycopy(transcodingState.inputs, 10, onlyDataInputs, 0, 2);
//        int[] onlyDataInputOffsets = new int[2];
//        System.arraycopy(transcodingState.inputOffsets, 10, onlyDataInputOffsets, 0, 2);
//
//        RSUtil.encodeData(gfTables, transcodingState.transcodeLength, onlyDataInputs,
//                onlyDataInputOffsets, transcodingState.outputs, transcodingState.outputOffsets);


        // the real bandwidth-optimal transcoding implementation.
        int newDataNum =  options.getNewDataNum();
        int newParityNum = options.getNewParityNum();
        int oldDataNum = options.getOldDataNum();
        int oldParityNum = options.getOldParityNum();

        int subBlockSize = transcodingState.transcodeLength / subPacketSize;
        int numExtraParities = newParityNum - oldParityNum;
        assert (newDataNum % oldDataNum == 0);
        int numOldStripes = newDataNum / oldDataNum;
        assert (transcodingState.inputs.length == (newDataNum + numOldStripes * oldParityNum));

        // all source parities
        byte[][][] allParities = new byte[numOldStripes][][];
        for (int i = 0; i < numOldStripes; ++i) {
            allParities[i] = new byte[newParityNum][];
            // we have all old parities (with or without embedded piggybacks).  new parities, not yet.
            for (int j = 0; j < oldParityNum; ++j) {
                allParities[i][j] = transcodingState.inputs[newDataNum +  i * oldParityNum + j];
            }
            for (int j = oldParityNum; j < newParityNum; ++j) {
                allParities[i][j] = new byte[transcodingState.transcodeLength];
            }
        }


        // step 1: For the last r_f - r_i sub stripes, compute all r_f parities.  Do this for all the stripes you are merging.  You need an encoder.
        byte[][][] paritiesWOPiggybacks = new byte[numOldStripes][][];
        for (int i = 0; i < numOldStripes; ++i) {
            paritiesWOPiggybacks[i] = new byte[newParityNum][];
            for (int j = 0; j < newParityNum; ++j) {
                paritiesWOPiggybacks[i][j] = new byte[subBlockSize * numExtraParities];
            }
        }

        // compute for all the stripes
        for (int i = 0; i < numOldStripes; ++i) {
            byte[][] dataBlocksForStripe = new byte[oldDataNum][];
            System.arraycopy(transcodingState.inputs, i * oldDataNum, dataBlocksForStripe, 0, oldDataNum);
            int[] dataOffsetsForStripe = new int[oldDataNum];
            // once you send the right offsets from the upper level, you can delete this.  offsets will then be 0 here.
//            for (int j = 0; j < oldDataNum; ++j) {
//                dataOffsetsForStripe[i] = subBlockSize * oldParityNum;
//            }
            int[] parityOffsets = new int[newParityNum];
            ByteArrayEncodingState baeForParitiesWOPiggybacks = new ByteArrayEncodingState(encoderForStripesWOPiggyback, subBlockSize * numExtraParities, dataBlocksForStripe, dataOffsetsForStripe, paritiesWOPiggybacks[i], parityOffsets);

            // CoderUtil.resetOutputBuffers(paritiesWOPiggybacks[i], parityOffsets, subBlockSize * numExtraParities);
            // RSUtil.encodeData(gfTables, subBlockSize * numExtraParities, dataBlocksForStripe, dataOffsetsForStripe, paritiesWOPiggybacks[i], parityOffsets);
            encoderForStripesWOPiggyback.doEncode(baeForParitiesWOPiggybacks);

            for (int j = oldParityNum; j < newParityNum; ++j) {
                System.arraycopy(paritiesWOPiggybacks[i][j], 0, allParities[i][j], subBlockSize * oldParityNum, subBlockSize * numExtraParities);
            }
        }


        // step 2: Compute Piggybacks for each stripe: XOR the newly computed parities with the parities on disk to get piggybacks.   The piggybacks are the r_f - r_i new parities for the first r_i substripes.
        // For each of the old stripes
        for (int i = 0; i < numOldStripes; ++i) {
            // for each of the last r_f - r_i substripes
            for (int j = 0; j < numExtraParities; ++j) {
                // for each of the old parities
                for (int k = 0; k < oldParityNum; ++k) {
                    for (int l = 0; l < subBlockSize; ++l) {
                        byte a = allParities[i][k][subBlockSize * (oldParityNum + j) + l];
                        byte b = paritiesWOPiggybacks[i][k][subBlockSize * j + l];
                        byte c = (byte) (a ^ b);
                        allParities[i][oldParityNum + j][subBlockSize * k + l] = c;
                    }
                }
            }

            for (int j = 0; j < oldParityNum; ++j) {
                System.arraycopy(paritiesWOPiggybacks[i][j], 0, allParities[i][j], subBlockSize * oldParityNum, subBlockSize * numExtraParities);
            }
        }

//        LOG.info("[BWOCC]-t After step 2: all parities");
//        for (int i = 0; i < numOldStripes; ++i) {
//            for (int j = 0; j < newParityNum; ++j) {
//                LOG.info("[BWOCC]-t doTranscode() step 2: Parities[{}][{}] = {} {} {} {} {}", i, j, allParities[i][j][0], allParities[i][j][1048576/4], allParities[i][j][1048576/2], allParities[i][j][1048576*3/4], allParities[i][j][1048576-1] );
//            }
//        }


        // step 3: You now have all r_f parities for all data stripes you want to merge, and can transcode like Access-Optimal Convertible Codes.
        byte[][] allParitiesTS = new byte[numOldStripes * newParityNum][];
        int totalParities = 0;
        for (int i = 0; i < numOldStripes; ++i) {
            for (int j = 0; j < newParityNum; ++j) {
                allParitiesTS[totalParities] = allParities[i][j];
                ++totalParities;
            }
        }
        int[] parityOffsetsTS = new int[allParitiesTS.length];
        CoderUtil.resetOutputBuffers(transcodingState.outputs, transcodingState.outputOffsets,
                transcodingState.transcodeLength);
        RSUtil.encodeData(gfTables, transcodingState.transcodeLength, allParitiesTS,
                parityOffsetsTS, transcodingState.outputs, transcodingState.outputOffsets);

//        for (int i = 0; i < newParityNum; ++i) {
//            LOG.info("[BWOCC]-t doTranscode() step 3: Parities[{}] = {} {} {} {} {}", i, transcodingState.outputs[i][0], transcodingState.outputs[i][1048576/4], transcodingState.outputs[i][1048576/2], transcodingState.outputs[i][1048576*3/4], transcodingState.outputs[i][1048576-1] );
//        }
    }
}
