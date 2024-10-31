package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.erasurecode.rawcoder.util.DumpUtil;
import org.apache.hadoop.io.erasurecode.rawcoder.util.RSUtil;
@InterfaceAudience.Private
public class CCRawEncoder extends RawErasureEncoder{

    private byte[] encodeMatrix;

    private byte[] gfTables;

    public CCRawEncoder(ErasureCoderOptions coderOptions) {
        super(coderOptions);

        LOG.info("Encoding with CCRawEncoder");

        if (getNumAllUnits() >= RSUtil.GF.getFieldSize()) {
            throw new HadoopIllegalArgumentException(
                    "Invalid numDataUnits and numParityUnits");
        }

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
    }

    @Override
    protected void doEncode(ByteBufferEncodingState encodingState) throws IOException {
        CoderUtil.resetOutputBuffers(encodingState.outputs,
                encodingState.encodeLength);
        RSUtil.encodeData(gfTables, encodingState.inputs, encodingState.outputs);
    }

    @Override
    protected void doEncode(ByteArrayEncodingState encodingState) throws IOException {
        CoderUtil.resetOutputBuffers(encodingState.outputs,
                encodingState.outputOffsets,
                encodingState.encodeLength);
        RSUtil.encodeData(gfTables, encodingState.encodeLength,
                encodingState.inputs,
                encodingState.inputOffsets, encodingState.outputs,
                encodingState.outputOffsets);

    }
}
