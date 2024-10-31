package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.util.DumpUtil;
import org.apache.hadoop.io.erasurecode.rawcoder.util.GF256;
import org.apache.hadoop.io.erasurecode.rawcoder.util.RSUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class BCCRawDecoder extends RawErasureDecoder{
    private byte[] encodeMatrix;
    private byte[] encodeMatrixFinal;

    /**
     * Below are relevant to schema and erased indexes, thus may change during
     * decode calls.
     */
    private byte[] decodeMatrix;
    private byte[] decodeMatrixFinal;
    private byte[] invertMatrix;
    private byte[] invertMatrixFinal;
    /**
     * Array of input tables generated from coding coefficients previously.
     * Must be of size 32*k*rows
     */
    private byte[] gfTables;
    private byte[] gfTablesFinal;
    private int[] cachedErasedIndexes;
    private int[] validIndexes;
    private int numErasedDataUnits;
    private boolean[] erasureFlags;
    private int numAllUnitsForEncoding;
    private int subPacketSize;
    private CCRawEncoder ccEncoder;

    public BCCRawDecoder(ErasureCoderOptions coderOptions) {
        super(coderOptions);

        int numAllUnits = getNumAllUnits();
        numAllUnitsForEncoding = coderOptions.getNumDataUnits() + coderOptions.getNumParityUnitsFinal();
        if (getNumAllUnits() >= RSUtil.GF.getFieldSize() && numAllUnitsForEncoding >= RSUtil.GF.getFieldSize()) {
            throw new HadoopIllegalArgumentException(
                    "Invalid getNumDataUnits() and numParityUnits");
        }

        encodeMatrix = new byte[numAllUnits * getNumDataUnits()];
        RSUtil.genRSMatrix(encodeMatrix, numAllUnits, getNumDataUnits());
        if (allowVerboseDump()) {
            DumpUtil.dumpMatrix(encodeMatrix, getNumDataUnits(), numAllUnits);
        }

        encodeMatrixFinal = new byte[numAllUnitsForEncoding * getNumDataUnits()];
        subPacketSize = getNumParityUnitsFinal();
        ErasureCoderOptions coderOptionsToComputeAllParities = new ErasureCoderOptions(coderOptions.getNumDataUnits(), coderOptions.getNumParityUnitsFinal());
        ccEncoder = new CCRawEncoder(coderOptionsToComputeAllParities);
        LOG.debug("[BWOCC]-d Size of encodeMatrixFinal = {}.  NumALlUnitsForEncoding = {}", encodeMatrixFinal.length, numAllUnitsForEncoding);
    }


    @Override
    protected void doDecode(ByteBufferDecodingState decodingState) throws IOException {
        CoderUtil.resetOutputBuffers(decodingState.outputs,
                decodingState.decodeLength);
        prepareDecoding(decodingState.inputs, decodingState.erasedIndexes);

        ByteBuffer[] realInputs = new ByteBuffer[getNumDataUnits()];
        for (int i = 0; i < getNumDataUnits(); i++) {
            realInputs[i] = decodingState.inputs[validIndexes[i]];
        }
        RSUtil.encodeData(gfTables, realInputs, decodingState.outputs);
    }

    // without cell-level striping.  Contiguous EC (cell size = block size).
    @Override
    protected void doDecode(ByteArrayDecodingState decodingState) throws IOException {
        LOG.info("[BWOCC]-d BCCRawDecoder.doDecode() with ByteArrayDecodingState for without cell-level striping.");
        LOG.info("[BWOCC] BAD decodingState.inputs.size() = {}x{};  outputs.size = {}x{};  inputOffsets.size() = {};  outputOffsets.size() = {}; encodeLength = {};", decodingState.inputs.length, decodingState.inputs[decodingState.inputs.length-1].length, decodingState.outputs.length, decodingState.outputs[0].length, decodingState.inputOffsets.length, decodingState.outputOffsets.length, decodingState.decodeLength);

        assert (decodingState.outputs.length == getNumParityUnits());

        // size of subStripe = subBlockSize * k
        int subBlockSize = decodingState.decodeLength / subPacketSize;
        int dataLen = subBlockSize;

        // step 1: for the first r_i substripes decode normally
        for (int i = 0; i < getNumParityUnits(); ++i) {
            Arrays.fill(decodingState.outputOffsets, i * subBlockSize);
            Arrays.fill(decodingState.inputOffsets, i * subBlockSize);
            CoderUtil.resetOutputBuffers(decodingState.outputs,
                    decodingState.outputOffsets, dataLen);
            LOG.info("[BWOCC]-d BCCRawDecoder:doDecode() step 1: resetOutputBuffers done.  Preparing decoding.");
            prepareDecoding(decodingState.inputs, decodingState.erasedIndexes);
            LOG.info("[BWOCC]-d BCCRawDecoder:doDecode() step 1: preparation done.");

            byte[][] realInputs = new byte[getNumDataUnits()][];
            int[] realInputOffsets = new int[getNumDataUnits()];
            for (int j = 0; j < getNumDataUnits(); j++) {
                realInputs[j] = decodingState.inputs[validIndexes[j]];
                realInputOffsets[j] = decodingState.inputOffsets[validIndexes[j]];
            }
            RSUtil.encodeData(gfTables, dataLen, realInputs, realInputOffsets,
                    decodingState.outputs, decodingState.outputOffsets);
        }
        LOG.info("[BWOCC]-d BCCRawDecoder:doDecode() step 1 done.  decoded normally for the first r_i substripes.");

        // step 2: for the first r_i substripes, compute the next (r_f - r_i) parities.
        // for now I am computing all r_f parities, even though we don't need it.
        // TODO: change RSUtil.encode to compute only last x parities.
        byte[][] extraParities = new byte[subPacketSize][];
        for (int i = 0; i < subPacketSize; ++i) {
            extraParities[i] = new byte[subBlockSize * getNumParityUnits()];
        }
        // prepare inputs. some elements of decodingState.inputs are currently NULL.
        byte[][] forExtraParitiesInputs = new byte[getNumDataUnits()][];
        int reconstructedBlockIndex = 0;
        for (int i = 0; i < getNumDataUnits(); ++i) {
            if (decodingState.inputs[i] == null) {
                forExtraParitiesInputs[i] = decodingState.outputs[reconstructedBlockIndex];
                ++reconstructedBlockIndex;
            } else {
                forExtraParitiesInputs[i] = decodingState.inputs[i];
            }
        }

        int[] forExtraParitiesInputOffsets = new int[forExtraParitiesInputs.length];
        int[] extraParitiesOffsets = new int[extraParities.length];
        ByteArrayEncodingState forExtraParitiesBAEState = new ByteArrayEncodingState(ccEncoder, subBlockSize * getNumParityUnits(), forExtraParitiesInputs, forExtraParitiesInputOffsets, extraParities, extraParitiesOffsets);
        ccEncoder.doEncode(forExtraParitiesBAEState);
        DumpUtil.dumpArrayOfBytes("CCEncoder for all parities", forExtraParitiesBAEState.outputs);

        LOG.info("[BWOCC]-d BCCRawDecoder:doDecode() step 2 done.  computing r_f parities for the first r_i substripes..");


        // step 3: xor computed parities in step 2 with corresponding parities in the next (r_f - r_i) substripes.
        // TODO: only do this for parities which are available.
        byte[][] paritiesWOPiggybacks = new byte[getNumParityUnits()][];
        for (int i = 0; i < getNumParityUnits(); ++i) {
            paritiesWOPiggybacks[i] = new byte[subBlockSize * (getNumParityUnitsFinal() - getNumParityUnits())];
        }
        LOG.info("[BWOCC]-d XORing parities with piggybacks.");
        // for each of the r_i parities
        for (int i = 0; i < getNumParityUnits(); ++i) {
            // for each of the remaining r_f - r_i subStripes( or subBlocks)
            for (int j = 0; j < getNumParityUnitsFinal() - getNumParityUnits(); ++j) {
                String indices = "i="+i+"/j="+j;
                LOG.info("[BWOCC]-d xoring: {}", indices);
                DumpUtil.dumpBytes("parities", decodingState.inputs[getNumDataUnits() + i]);
                DumpUtil.dumpBytes("piggybacks", extraParities[getNumParityUnits() + j]);
                DumpUtil.dumpBytes("piggybacks calculated", forExtraParitiesBAEState.outputs[getNumParityUnits() + j]);
                for (int k = 0; k < subBlockSize; ++k) {
                    paritiesWOPiggybacks[i][subBlockSize * j + k] = (byte) (decodingState.inputs[getNumDataUnits() + i][subBlockSize * (getNumParityUnits() + j) + k] ^ extraParities[getNumParityUnits() + j][subBlockSize * i + k]);
                }
            }
        }
        DumpUtil.dumpArrayOfBytes("Parities w/o Piggybacks: ", paritiesWOPiggybacks);
        LOG.info("[BWOCC]-d BCCRawDecoder:doDecode() step 3 done.  xor piggybacks with parities.");

        // step 4: we can now normally decode the remaining (r_f - r_i) substripes.
        {
            Arrays.fill(decodingState.outputOffsets, getNumParityUnits() * subBlockSize);
            Arrays.fill(decodingState.inputOffsets, getNumParityUnits() * subBlockSize);
            for (int i = getNumDataUnits(), j=0; i < getNumDataUnits() + getNumParityUnits(); ++i, ++j) {
                if (decodingState.inputs[i] != null) {
                    decodingState.inputs[i] = paritiesWOPiggybacks[j];
                    decodingState.inputOffsets[i] = 0;
                }
            }
            dataLen = subBlockSize * (getNumParityUnitsFinal() - getNumParityUnits());
            CoderUtil.resetOutputBuffers(decodingState.outputs,
                    decodingState.outputOffsets, dataLen);
            prepareDecoding(decodingState.inputs, decodingState.erasedIndexes);

            byte[][] realInputs = new byte[getNumDataUnits()][];
            int[] realInputOffsets = new int[getNumDataUnits()];
            for (int i = 0; i < getNumDataUnits(); i++) {
                realInputs[i] = decodingState.inputs[validIndexes[i]];
                realInputOffsets[i] = decodingState.inputOffsets[validIndexes[i]];
            }
            RSUtil.encodeData(gfTables, dataLen, realInputs, realInputOffsets,
                    decodingState.outputs, decodingState.outputOffsets);

        }

        // step 5: if dividing block into subBlocks leaves remainder cells, take care of them normally.
        if (decodingState.decodeLength % subPacketSize > 0) {
            int remBlockSize = decodingState.decodeLength - ( decodingState.decodeLength % subPacketSize );
            Arrays.fill(decodingState.outputOffsets, subPacketSize * subBlockSize);
            Arrays.fill(decodingState.inputOffsets, subPacketSize * subBlockSize);
            dataLen = remBlockSize;
            CoderUtil.resetOutputBuffers(decodingState.outputs,
                    decodingState.outputOffsets, dataLen);
            prepareDecoding(decodingState.inputs, decodingState.erasedIndexes);

            byte[][] realInputs = new byte[getNumDataUnits()][];
            int[] realInputOffsets = new int[getNumDataUnits()];
            for (int i = 0; i < getNumDataUnits(); i++) {
                realInputs[i] = decodingState.inputs[validIndexes[i]];
                realInputOffsets[i] = decodingState.inputOffsets[validIndexes[i]];
            }
            RSUtil.encodeData(gfTables, dataLen, realInputs, realInputOffsets,
                    decodingState.outputs, decodingState.outputOffsets);
        }
    }

    private <T> void prepareDecoding(T[] inputs, int[] erasedIndexes) {
        int[] tmpValidIndexes = CoderUtil.getValidIndexes(inputs);
        if (Arrays.equals(this.cachedErasedIndexes, erasedIndexes) &&
                Arrays.equals(this.validIndexes, tmpValidIndexes)) {
            return; // Optimization. Nothing to do
        }
        this.cachedErasedIndexes =
                Arrays.copyOf(erasedIndexes, erasedIndexes.length);
        this.validIndexes =
                Arrays.copyOf(tmpValidIndexes, tmpValidIndexes.length);

        processErasures(erasedIndexes);
    }

    private void processErasures(int[] erasedIndexes) {
        this.decodeMatrix = new byte[getNumAllUnits() * getNumDataUnits()];
        this.invertMatrix = new byte[getNumAllUnits() * getNumDataUnits()];
        this.gfTables = new byte[getNumAllUnits() * getNumDataUnits() * 32];

        this.erasureFlags = new boolean[getNumAllUnits()];
        this.numErasedDataUnits = 0;

        for (int i = 0; i < erasedIndexes.length; i++) {
            int index = erasedIndexes[i];
            erasureFlags[index] = true;
            if (index < getNumDataUnits()) {
                numErasedDataUnits++;
            }
        }

        generateDecodeMatrix(erasedIndexes);

        RSUtil.initTables(getNumDataUnits(), erasedIndexes.length,
                decodeMatrix, 0, gfTables);
        if (allowVerboseDump()) {
            System.out.println(DumpUtil.bytesToHex(gfTables, -1));
        }
    }

    // Generate decode matrix from encode matrix
    private void generateDecodeMatrix(int[] erasedIndexes) {
        int i, j, r, p;
        byte s;
        byte[] tmpMatrix = new byte[getNumAllUnits() * getNumDataUnits()];

        // Construct matrix tmpMatrix by removing error rows
        for (i = 0; i < getNumDataUnits(); i++) {
            r = validIndexes[i];
            for (j = 0; j < getNumDataUnits(); j++) {
                tmpMatrix[getNumDataUnits() * i + j] =
                        encodeMatrix[getNumDataUnits() * r + j];
            }
        }

        GF256.gfInvertMatrix(tmpMatrix, invertMatrix, getNumDataUnits());

        for (i = 0; i < numErasedDataUnits; i++) {
            for (j = 0; j < getNumDataUnits(); j++) {
                decodeMatrix[getNumDataUnits() * i + j] =
                        invertMatrix[getNumDataUnits() * erasedIndexes[i] + j];
            }
        }

        for (p = numErasedDataUnits; p < erasedIndexes.length; p++) {
            for (i = 0; i < getNumDataUnits(); i++) {
                s = 0;
                for (j = 0; j < getNumDataUnits(); j++) {
                    s ^= GF256.gfMul(invertMatrix[j * getNumDataUnits() + i],
                            encodeMatrix[getNumDataUnits() * erasedIndexes[p] + j]);
                }
                decodeMatrix[getNumDataUnits() * p + i] = s;
            }
        }
    }
}
