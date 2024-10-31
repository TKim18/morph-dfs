package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hadoop.io.erasurecode.coder.util.HHUtil;
import org.apache.hadoop.io.erasurecode.rawcoder.util.DumpUtil;
import org.apache.hadoop.io.erasurecode.rawcoder.util.RSUtil;
@InterfaceAudience.Private
public class BCCRawEncoder extends RawErasureEncoder{

    private byte[] encodeMatrix;
    private byte[] encodeMatrixFinal;

    private byte[] gfTables;
    private byte[] gfTablesFinal;

    private int subPacketSize;  // = r_f
    private int[] piggyBackIndex;

    // to store the "extra" 'r_f - r_i' parities for the first r_i substripes.
    private byte[][][] piggybacks;
    private int numRowsInSubstripe;
//    private BCCRawDecoder decoder;

    public BCCRawEncoder(ErasureCoderOptions coderOptions) {
        super(coderOptions);

        int numAllUnitsForEncoding = coderOptions.getNumDataUnits() + coderOptions.getNumParityUnitsFinal();

        LOG.debug("[BWOCC]  BCCRawEncoder() constructor dataUnits = {}; parityUnits = {}; dataUnitsFinal = {}; parityUnitsFinal = {}; numCellsInBlock = {}", coderOptions.getNumDataUnits(), coderOptions.getNumParityUnits(), coderOptions.getNumDataUnitsFinal(), coderOptions.getNumParityUnitsFinal(), coderOptions.getNumCellsInBlock());


        if (getNumAllUnits() >= RSUtil.GF.getFieldSize() && numAllUnitsForEncoding >= RSUtil.GF.getFieldSize()) {
            throw new HadoopIllegalArgumentException(
                    "Invalid numDataUnits and numParityUnits");
        }

        // Use CC to compute all r_f parities.
        encodeMatrix = new byte[getNumAllUnits() * getNumDataUnits()];
        RSUtil.genRSMatrix(encodeMatrix, getNumAllUnits(), getNumDataUnits());
        if (allowVerboseDump()) {
            DumpUtil.dumpMatrix(encodeMatrix, getNumDataUnits(), getNumAllUnits());
        }
        gfTables = new byte[getNumAllUnits() * getNumDataUnits() * 32];
        RSUtil.initTables(getNumDataUnits(), getNumParityUnits(), encodeMatrix,
                getNumDataUnits() * getNumDataUnits(), gfTables);
        if (allowVerboseDump()) {
            System.out.println(DumpUtil.bytesToHex(gfTables, -1));
        }

        LOG.debug("Normal CC matrices computed.");

        encodeMatrixFinal = new byte[numAllUnitsForEncoding * getNumDataUnits()];
        LOG.debug("[BWOCC] Size of encodeMatrixFinal = {}.  numAllUnitsForEncoding = {}", encodeMatrixFinal.length, numAllUnitsForEncoding);
        RSUtil.genRSMatrix(encodeMatrixFinal, numAllUnitsForEncoding, getNumDataUnits());
        if (allowVerboseDump()) {
            DumpUtil.dumpMatrix(encodeMatrixFinal, getNumDataUnits(), numAllUnitsForEncoding);
        }
        gfTablesFinal = new byte[numAllUnitsForEncoding * getNumDataUnits() * 32];
        RSUtil.initTables(getNumDataUnits(), getNumParityUnitsFinal(), encodeMatrixFinal,
                getNumDataUnits() * getNumDataUnits(), gfTablesFinal);
        if (allowVerboseDump()) {
            System.out.println(DumpUtil.bytesToHex(gfTablesFinal, -1));
        }

        LOG.debug("Extended CC matrices computed.");

        subPacketSize = coderOptions.getNumParityUnitsFinal();
//        piggyBackIndex = HHUtil.initPiggyBackIndexWithoutPBVec(getNumDataUnits(), getNumParityUnitsFinal());
        // for each of the first r_i (getNumParityUnits()) substripes, we will store 'r_f - r_i' buffers (byte[]).
        // If final parities are null, this ECPolicy is only being used for post-transcoding target.   We don't care about numRowsInSubstripe.
        if (subPacketSize != 0) {
            numRowsInSubstripe = coderOptions.getNumCellsInBlock() / subPacketSize;
        }
        else{
            numRowsInSubstripe = 0;
        }
        boolean cellLevelStriping = false;
        if (cellLevelStriping) {
            // Each substripe is composed of numRowsInSubstripe rows of data.
            // for each row in first r_i substripes, r_f - r_i parities will be stored.  Each parity would be of size cellSize.
            piggybacks = new byte[ numRowsInSubstripe*getNumParityUnits() ][][];
        }

//        decoder = new BCCRawDecoder(coderOptions);


    }

    @Override
    protected void doEncode(ByteBufferEncodingState encodingState) throws IOException {
        CoderUtil.resetOutputBuffers(encodingState.outputs,
                encodingState.encodeLength);
        LOG.debug("[BWOCC] BCCRawEncoder.doEncode() with ByteBufferEncodingState");
        RSUtil.encodeData(gfTables, encodingState.inputs, encodingState.outputs);
        throw new IOException("ByteBuffers not yet supported by BCC.");
    }

    // without cell level striping.  Contiguous EC (or cell size = block size)
    @Override
    protected void doEncode(ByteArrayEncodingState encodingState) throws IOException {
        LOG.debug("[BWOCC] BCCRawEncoder.doEncode() encodingState.inputs.size() = {}x{};  outputs.size = {}x{};  inputOffsets.size() = {};  outputOffsets.size() = {}; encodeLength = {}; r_i substripes = {}", encodingState.inputs.length, encodingState.inputs[0].length, encodingState.outputs.length, encodingState.outputs[0].length, encodingState.inputOffsets.length, encodingState.outputOffsets.length, encodingState.encodeLength, getNumParityUnits() * numRowsInSubstripe);

        // for now, we are only supporting the case r_f > r_i
        assert (encodingState.outputs.length < subPacketSize);
        assert (encodingState.outputs.length == getNumParityUnits());

        // size of subStripe = subBlockSize * k
        int subBlockSize = encodingState.encodeLength / subPacketSize;

        byte[][] extendedOutputs = new byte[subPacketSize][];
        // the first r_i parities are to be returned as is in encodingState.outputs.
        // the first r_i (encodingState.outputs.length) elements will therefore refer to
        // elements of encodingState.outputs, buffers already allocated.
        System.arraycopy(encodingState.outputs, 0, extendedOutputs, 0, encodingState.outputs.length);
        // the remaining r_f - r_i parities are piggybacks; allocate new buffers.
        for (int j = encodingState.outputs.length; j < subPacketSize; ++j) {
            extendedOutputs[j] = new byte[encodingState.encodeLength];
        }
        LOG.debug("[BWOCC] doEncode(): outputs[0].length = {}; extendedOutputs.lengths = {}, {}",
                encodingState.outputs[0].length, extendedOutputs[0].length, extendedOutputs[1].length);
        int[] extendedOutputOffsets = new int[subPacketSize];  // the default values will be 0.

        // step 1:  for the first r_i substripes, compute all r_f parities, save piggybacks.
        for (int i = 0; i < encodingState.outputs.length; ++i) {
            // init offsets.
            for (int j = 0; j < subPacketSize; ++j) {
                extendedOutputOffsets[j] = i * subBlockSize;
            }
            Arrays.fill(encodingState.inputOffsets, i * subBlockSize);

            CoderUtil.resetOutputBuffers(extendedOutputs,
                    extendedOutputOffsets,
                    subBlockSize);
            RSUtil.encodeData(gfTablesFinal, subBlockSize,
                    encodingState.inputs,
                    encodingState.inputOffsets, extendedOutputs,
                    extendedOutputOffsets);
        }

        // step 2:  for the remaining r_f - r_i substripes, add piggyback.
        for (int i = encodingState.outputs.length; i < subPacketSize; ++i) {
            // init offsets.
            Arrays.fill(encodingState.outputOffsets, i * subBlockSize);
            Arrays.fill(encodingState.inputOffsets, i * subBlockSize);

            CoderUtil.resetOutputBuffers(encodingState.outputs,
                    encodingState.outputOffsets,
                    subBlockSize);
            RSUtil.encodeData(gfTables, subBlockSize,
                    encodingState.inputs,
                    encodingState.inputOffsets, encodingState.outputs,
                    encodingState.outputOffsets);

            // each of the r_i parities of this substripe will be embedded with a piggyback
            for (int j = 0; j < getNumParityUnits(); ++j) {  // r_i
                for (int k = i*subBlockSize, l = j*subBlockSize ; k < (i+1)*subBlockSize; ++k, ++l) {
                    encodingState.outputs[j][k] ^= extendedOutputs[i][l];
                }
            }
        }

        // step 3: if dividing block into subBlocks leaves remainder cells, take care of them normally.
        if (encodingState.encodeLength % subPacketSize > 0) {
            int remBlockSize = encodingState.encodeLength - ( encodingState.encodeLength % subPacketSize );
            Arrays.fill(encodingState.outputOffsets, subPacketSize * subBlockSize);
            Arrays.fill(encodingState.inputOffsets, subPacketSize * subBlockSize);

            CoderUtil.resetOutputBuffers(encodingState.outputs,
                    encodingState.outputOffsets,
                    remBlockSize);
            RSUtil.encodeData(gfTables, remBlockSize,
                    encodingState.inputs,
                    encodingState.inputOffsets, encodingState.outputs,
                    encodingState.outputOffsets);
        }



        // verify decoding here.


    }

    // with cell-level striping
    @Override
    protected void doEncode(ByteArrayEncodingState encodingState, int atRow) throws IOException {
        LOG.debug("[BWOCC] BCCRawEncoder.doEncode() encodingStat.inputs.size() = {}x{};  outputs.size = {}x{};  inputOffsets.size() = {};  outputOffsets.size() = {}; encodeLength = {}; atRow = {};  r_i substripes = {}", encodingState.inputs.length, encodingState.inputs[0].length, encodingState.outputs.length, encodingState.outputs[0].length, encodingState.inputOffsets.length, encodingState.outputOffsets.length, encodingState.encodeLength, atRow, getNumParityUnits() * numRowsInSubstripe);

        // for the first r_i substripes, compute
        if (atRow < getNumParityUnits() * numRowsInSubstripe) {
            byte[][] extendedOutputs = new byte [subPacketSize][];
            assert (encodingState.outputs.length < subPacketSize);
            System.arraycopy(encodingState.outputs, 0, extendedOutputs, 0, encodingState.outputs.length);
            for (int i = encodingState.outputs.length; i < subPacketSize; ++i) {
                extendedOutputs[i] = new byte[encodingState.encodeLength];
            }
            int[] extendedOutputOffsets = new int[subPacketSize];

            CoderUtil.resetOutputBuffers(extendedOutputs,
                    extendedOutputOffsets,
                    encodingState.encodeLength);
            RSUtil.encodeData(gfTablesFinal, encodingState.encodeLength,
                    encodingState.inputs,
                    encodingState.inputOffsets, extendedOutputs,
                    extendedOutputOffsets);

            String exparities = "";
            for (int i = 0; i < extendedOutputs.length; ++i) {
                exparities += extendedOutputs[i][0];
                exparities += ", ";
            }
            String parities = "";
            for (int i = 0; i < encodingState.outputs.length; ++i) {
                parities += encodingState.outputs[i][0];
                parities += ", ";
            }
            LOG.debug("[BWOCC] encoding results: extendedOutputs = {}; outputs = {}", exparities, parities);

            // extendedOutputOffsets now contains all r_f parities.
            // store the last r_f - r_i parity buffers in 'piggybacks'
            int numExtraParities = getNumParityUnitsFinal() - getNumParityUnits();
            piggybacks[atRow] = new byte[numExtraParities][];
            for (int i = getNumParityUnits(); i < getNumParityUnitsFinal(); ++i) {
                piggybacks[atRow][i - getNumParityUnits()] = extendedOutputs[i];
            }
        }

        // for the remaining 'r_f - r_i' substripes, embed the piggybacks onto the r_i parities
        else {
            CoderUtil.resetOutputBuffers(encodingState.outputs,
                    encodingState.outputOffsets,
                    encodingState.encodeLength);
            RSUtil.encodeData(gfTables, encodingState.encodeLength,
                    encodingState.inputs,
                    encodingState.inputOffsets, encodingState.outputs,
                    encodingState.outputOffsets);

            // each of the r_i parities of this substripe will be embedded with a piggyback
            LOG.debug("OutOfBounds error:  getNumParityUnits() = {}; encodeLength = {}, outputs.length = {}x{}, piggybacks.length = {}x{}x{}; atRow = {}", getNumParityUnits(), encodingState.encodeLength, encodingState.outputs.length, encodingState.outputs[0].length, piggybacks.length, piggybacks[0].length, piggybacks[0][0].length, atRow);
            int extraParities = getNumParityUnitsFinal() - getNumParityUnits();
            for (int i = 0; i < getNumParityUnits(); ++i) { // r_i
                LOG.debug("Adding piggybacks: i = {}, atRow - r = {}", i, atRow - numRowsInSubstripe*getNumParityUnits());
                int z = (atRow - numRowsInSubstripe*getNumParityUnits()) * getNumParityUnits() + i;
                for (int j = 0; j < encodingState.encodeLength; ++j) {
                    encodingState.outputs[i][j] ^= piggybacks[(int) z/extraParities][z % extraParities][j];
                }
            }
        }
    }
}
