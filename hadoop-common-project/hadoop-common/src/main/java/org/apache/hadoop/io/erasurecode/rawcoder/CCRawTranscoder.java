package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.io.erasurecode.rawcoder.util.RSUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CCRawTranscoder extends RawErasureTranscoder {

    static final Logger LOG = LoggerFactory.getLogger(CCRawTranscoder.class);

    private final byte[] transcodeMatrix;
    private final byte[] gfTables;

    public CCRawTranscoder(TranscoderOptions options){
        super(options);
        //We're going to need a parity to field size check, I think

        transcodeMatrix = new byte[options.getMergeFactor() * options.getNewParityNum() * options.getNewParityNum()];
        RSUtil.genCCTranscodingMatrix(transcodeMatrix, options.getMergeFactor(), options.getNewParityNum(), options.getOldDataNum());

        gfTables = new byte[options.getMergeFactor() * options.getNewParityNum() * options.getNewParityNum() * 32];
        RSUtil.initTables(options.getMergeFactor() * options.getNewParityNum(), options.getNewParityNum(),
                transcodeMatrix, 0, gfTables);
    }

    protected void doTranscode(ByteBufferTranscodingState transcodingState){
        CoderUtil.resetOutputBuffers(transcodingState.outputs,
                transcodingState.transcodeLength);
        RSUtil.encodeData(gfTables, transcodingState.inputs, transcodingState.outputs);
    }

    protected void doTranscode(ByteArrayTranscodingState transcodingState){
        CoderUtil.resetOutputBuffers(transcodingState.outputs, transcodingState.outputOffsets,
                transcodingState.transcodeLength);
        RSUtil.encodeData(gfTables, transcodingState.transcodeLength, transcodingState.inputs,
                transcodingState.inputOffsets, transcodingState.outputs, transcodingState.outputOffsets);
    }
}
