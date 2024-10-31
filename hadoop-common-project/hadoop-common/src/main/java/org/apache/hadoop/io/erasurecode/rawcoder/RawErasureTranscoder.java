package org.apache.hadoop.io.erasurecode.rawcoder;

import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class RawErasureTranscoder {
    protected final TranscoderOptions options;

    public RawErasureTranscoder(TranscoderOptions options){
        this.options = options;
    }

    public void transcode(ByteBuffer[] inputs, ByteBuffer[] outputs) throws IOException {
        ByteBufferTranscodingState bbtState = new ByteBufferTranscodingState(
                this, inputs, outputs);

        boolean usingDirectBuffer = bbtState.usingDirectBuffer;
        int dataLen = bbtState.transcodeLength;
        if (dataLen == 0) {
            return;
        }

        int[] inputPositions = new int[inputs.length];
        for (int i = 0; i < inputPositions.length; i++) {
            if (inputs[i] != null) {
                inputPositions[i] = inputs[i].position();
            }
        }

        if (usingDirectBuffer) {
            doTranscode(bbtState);
        } else {
            ByteArrayTranscodingState batState = bbtState.convertToByteArrayState();
            doTranscode(batState);
        }

        for (int i = 0; i < inputs.length; i++) {
            if (inputs[i] != null) {
                // dataLen bytes consumed
                inputs[i].position(inputPositions[i] + dataLen);
            }
        }
    }

    protected abstract void doTranscode(ByteBufferTranscodingState transcodingState) throws IOException;

    protected abstract void doTranscode(ByteArrayTranscodingState transcodingState) throws IOException;

    public boolean preferDirectBuffer() {
        return false;
    }

    public void release(){

    }

}
