package org.apache.hadoop.io.erasurecode.rawcoder;

public class ByteArrayTranscodingState extends TranscodingState {
    byte[][] inputs;
    byte[][] outputs;

    int[] inputOffsets;
    int[] outputOffsets;

    public ByteArrayTranscodingState(RawErasureTranscoder transcoder, byte[][] inputs, byte[][] outputs){
        this.transcoder = transcoder;
        this.inputs = inputs;
        this. outputs = outputs;
        byte[] validInput = CoderUtil.findFirstValidInput(inputs);
        this.transcodeLength = validInput.length;

        this.inputOffsets = new int[inputs.length]; // ALL ZERO
        this.outputOffsets = new int[outputs.length]; // ALL ZERO
    }

    public ByteArrayTranscodingState(RawErasureTranscoder transcoder, int transcodeLength, byte[][] inputs,
                                     int[] inputOffsets, byte[][] outputs, int[] outputOffsets){
        this.transcoder = transcoder;
        this.inputs = inputs;
        this.outputs = outputs;
        this.transcodeLength = transcodeLength;
        this.inputOffsets = inputOffsets;
        this.outputOffsets = outputOffsets;
    }

}
