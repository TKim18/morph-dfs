package org.apache.hadoop.io.erasurecode.rawcoder;

import java.nio.ByteBuffer;

public class ByteBufferTranscodingState extends TranscodingState {

    ByteBuffer[] inputs;
    ByteBuffer[] outputs;
    boolean usingDirectBuffer;

    ByteBufferTranscodingState(RawErasureTranscoder transcoder,
                               ByteBuffer[] inputs, ByteBuffer[] outputs){
        this.transcoder = transcoder;

        this.inputs = inputs;
        this.outputs = outputs;
        ByteBuffer validInput = CoderUtil.findFirstValidInput(inputs);
        this.transcodeLength = validInput.remaining();
        this.usingDirectBuffer = validInput.isDirect();
    }

    ByteArrayTranscodingState convertToByteArrayState() {
        int[] inputOffsets = new int[inputs.length];
        int[] outputOffsets = new int[outputs.length];
        byte[][] newInputs = new byte[inputs.length][];
        byte[][] newOutputs = new byte[outputs.length][];

        ByteBuffer buffer;
        for (int i = 0; i < inputs.length; ++i) {
            buffer = inputs[i];
            if (buffer != null) {
                inputOffsets[i] = buffer.arrayOffset() + buffer.position();
                newInputs[i] = buffer.array();
            }
        }

        for (int i = 0; i < outputs.length; ++i) {
            buffer = outputs[i];
            outputOffsets[i] = buffer.arrayOffset() + buffer.position();
            newOutputs[i] = buffer.array();
        }

        ByteArrayTranscodingState batState = new ByteArrayTranscodingState(transcoder,
                transcodeLength, newInputs, inputOffsets, newOutputs, outputOffsets);
        return batState;
    }

}
