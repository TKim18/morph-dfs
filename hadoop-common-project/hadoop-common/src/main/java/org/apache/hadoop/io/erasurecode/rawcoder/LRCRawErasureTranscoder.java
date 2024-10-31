package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.io.erasurecode.rawcoder.util.RSUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

public class LRCRawErasureTranscoder extends RawErasureTranscoder {

    static final Logger LOG = LoggerFactory.getLogger(CCRawTranscoder.class);
    private final byte[] transcodeMatrix;
    private final byte[] gfTables;
    public LRCRawErasureTranscoder(TranscoderOptions options){
        super(options);
        int allNewParityNum = options.getNewLocalParityNum() + options.getNewParityNum();
        transcodeMatrix = new byte[allNewParityNum * options.getMergeFactor() * options.getOldParityNum()];
        RSUtil.genLRCTranscodingMatrix(transcodeMatrix, options.getMergeFactor(), options.getOldParityNum(),
                options.getNewParityNum(), options.getOldDataNum());

        this.gfTables = new byte[allNewParityNum * options.getMergeFactor() * options.getOldParityNum() * 32];
        RSUtil.initTables(options.getMergeFactor() * options.getOldParityNum(), allNewParityNum, transcodeMatrix,
                0, gfTables);
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
