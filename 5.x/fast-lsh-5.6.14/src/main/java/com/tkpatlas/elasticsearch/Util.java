package com.tkpatlas.elasticsearch;

import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.IntBuffer;
import java.util.Base64;

/**
 * Created by Lior Knaany on 4/7/18.
 * Updated by Reza on 7/2/19.
 */
public class Util {

    public static final int[] convertBase64ToArray(String base64Str) {
        final byte[] decode = Base64.getDecoder().decode(base64Str.getBytes());
        final IntBuffer intBuffer = ByteBuffer.wrap(decode).asIntBuffer();

        final int[] dims = new int[intBuffer.capacity()];
        intBuffer.get(dims);
        return dims;
    }

    public static final String convertArrayToBase64(int[] array) {
        final int capacity = 8 * array.length;
        final ByteBuffer bb = ByteBuffer.allocate(capacity);
        for (int i = 0; i < array.length; i++) {
            bb.putInt(array[i]);
        }
        bb.rewind();
        final ByteBuffer encodedBB = Base64.getEncoder().encode(bb);
        return new String(encodedBB.array());
    }
}
