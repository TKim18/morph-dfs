package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.io.erasurecode.rawcoder.util.RSUtil;

import java.io.IOException;

public class RSRawErasureTranscoder extends RawErasureTranscoder {
    private final byte[] newEncodingMatrix;
    private final byte[] gfTables;
    public RSRawErasureTranscoder(TranscoderOptions options){
        super(options);
        newEncodingMatrix = new byte[options.getAllNewUnits() * options.getNewDataNum()];
        RSUtil.genCauchyMatrix(newEncodingMatrix, options.getAllNewUnits(), options.getNewDataNum());

        this.gfTables = new byte[options.getAllNewUnits() * options.getNewDataNum() * 32];
        RSUtil.initTables(options.getNewDataNum(), options.getNewParityNum(), newEncodingMatrix,
                options.getNewDataNum() * options.getNewDataNum(),gfTables);
    }
    @Override
    protected void doTranscode(ByteBufferTranscodingState transcodingState) throws IOException {
        CoderUtil.resetOutputBuffers(transcodingState.outputs,
                transcodingState.transcodeLength);
        RSUtil.encodeData(gfTables, transcodingState.inputs, transcodingState.outputs);
    }

    @Override
    protected void doTranscode(ByteArrayTranscodingState transcodingState) throws IOException {
        CoderUtil.resetOutputBuffers(transcodingState.outputs, transcodingState.outputOffsets,
                transcodingState.transcodeLength);
        RSUtil.encodeData(gfTables, transcodingState.transcodeLength,
                transcodingState.inputs, transcodingState.inputOffsets, transcodingState.outputs,
                transcodingState.outputOffsets);
    }

//    private void createDecodingMatrix(TranscoderOptions options, byte[] decodingMatrix, byte[] oldEncodingMatrix){
//        this.decodingMatrix = new byte[options.getAllOldUnits() * options.getOldDataNum()];
//        GF256.gfInvertMatrix(oldEncodingMatrix, decodingMatrix, options.getOldDataNum());
//        this.gfTables = new byte[32 * options.getAllOldUnits()*options.getOldDataNum()];
//        RSUtil.initTables(options.getOldDataNum(), options.getOldParityNum(),
//                decodingMatrix, 0, gfTables);
//
//    }
}
