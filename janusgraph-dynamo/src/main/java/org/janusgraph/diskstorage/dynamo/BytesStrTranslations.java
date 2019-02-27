// Copyright 2019 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.dynamo;

import org.janusgraph.diskstorage.StaticBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BytesStrTranslations {
    private static final Logger LOG = LoggerFactory.getLogger(BytesStrTranslations.class);

    // Need something to be a null marker since I can't store a null value in a byte field and the AttributeUpdate
    // class doesn't have a method to set an attribute null.  I can't use any value with < 64 bits as the column could
    // legitimately be any of those values.  So you a 9 byte value.  Don't start it with 0 or 0xff as those are likely
    // to be more common and I want the equality to fail as quickly as possible since I'll have to check every
    // return value against this.
    static final byte[] NULL_BYTE_ARRAY_VALUE = new byte[] {(byte)(0x80),(byte)(0x80),(byte)(0x80),(byte)(0x80),(byte)(0x80),(byte)(0x80),(byte)(0x80),(byte)(0x80),(byte)(0x80)};
    static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    /**
     * Produces a String double the length of the array from the StaticBuffer.  Each byte from the
     * static buffer is stored in 2 characters
     */
    static StaticBuffer.Factory<String> bytesToStrFactory = BytesStrTranslations::bytesToStr;

    /**
     * Produces a String double the length of the array from the StaticBuffer with one added to the value of the
     * right most byte (with any carrying handled).
     */
    static StaticBuffer.Factory<String> bytesToStrMinusOneFactory = BytesStrTranslations::bytesToStrMinus1;

    /**
     * Translate a byte array into a hexidecimal String.  Each byte will occupy two String characters.
     * The byte array will be treated as if the bytes are unsigned.  That
     * is a byte value of -1 will be translated to 0xff.
     * @param bytes input array
     * @param offset starting position to translate, inclusive
     * @param limit stopping position for the translation, exclusive
     * @return a String
     */
    static String bytesToStr(byte[] bytes, int offset, int limit) {
        assert limit > offset;

        if (LOG.isTraceEnabled()) {
            logBytes("bytesToStr called with offset " + offset + " and limit " + limit, bytes);
        }
        // Not at all sure this is the most efficient way to do this
        StringBuilder buf = new StringBuilder((limit - offset) * 2);
        for (int i = offset; i < limit; i++) {
            String s = Integer.toHexString(Byte.toUnsignedInt(bytes[i]));
            if (s.length() == 1) buf.append('0');
            buf.append(s);
        }
        return buf.toString();
    }

    /**
     * Translate a byte array into a hexidecimal String.  Equivalent to {@link #bytesToStr(byte[], int, int)} of
     * (bytes, 0, bytes.length).
     * @param bytes input array
     * @return String
     */
    static String bytesToStr(byte[] bytes) {
        return bytesToStr(bytes, 0, bytes.length);
    }

    /**
     * Subtract one from a byte array as we convert it to a string.  This is necessary because Dynamo only offers between
     * (which is inclusive on both ends) and we need exclusive on the end key.
     * @param bytes input array
     * @param offset starting position to translate, inclusive
     * @param limit stopping position for the translation, exclusive
     * @return a String with value one higher than the passed in byte array
     */
    static String bytesToStrMinus1(byte[] bytes, int offset, int limit) {
        StringBuilder buf = new StringBuilder((limit - offset) * 2 + 1);
        boolean needToBorrow = true; // true the first time so that we subtract one
        for (int i = limit - 1; i >= offset; i--) {
            int hex = Byte.toUnsignedInt(bytes[i]);
            if (needToBorrow) hex--;
            if (hex < 0) {
                hex = 0xff;
                needToBorrow = true;
            } else {
                needToBorrow = false;
            }
            String s = Integer.toHexString(hex);
            buf.insert(0, s);
            if (s.length() == 1) buf.insert(0, '0');
        }
        if (needToBorrow) {
            // This should only happen if we're all zero all the way, in which case just return 0
            return "00";
        }
        return buf.toString();
    }

    /**
     * Add one to a byte array as we convert it to a string.  This is equivalent to
     * {@link #bytesToStrMinus1(byte[], int, int)} (bytes, 0, bytes.len).
     * @param bytes input array
     * @return a String with value one higher than the passed in byte array
     */
    static String bytesToStrMinus1(byte[] bytes) {
        return bytesToStrMinus1(bytes, 0, bytes.length);
    }

    /**
     * Translate a string representing unsigned bytes back into bytes
     * @param str String as stored in Dynamo.
     * @return regular byte array used by JG
     */
    static byte[] strToBytes(String str) {
        assert str.length() % 2 == 0;

        byte[] signed = new byte[str.length() / 2];
        for (int i = 0; i < signed.length; i++) {
            signed[i] = Integer.valueOf(Integer.parseInt(str.substring(i * 2, i * 2 + 2), 16)).byteValue();
        }
        return signed;
    }

    static void logBytes(String title, byte[] bytes) {
        StringBuilder buf = new StringBuilder(title);
        for (byte b : bytes) buf.append(' ').append(b);
        LOG.debug(buf.toString());
    }

}
