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

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;

import static org.janusgraph.diskstorage.dynamo.BytesStrTranslations.bytesToStr;
import static org.janusgraph.diskstorage.dynamo.BytesStrTranslations.bytesToStrMinus1;
import static org.janusgraph.diskstorage.dynamo.BytesStrTranslations.logBytes;
import static org.janusgraph.diskstorage.dynamo.BytesStrTranslations.strToBytes;

public class BytesToStrTest {
    private static final Logger LOG = LoggerFactory.getLogger(BytesToStrTest.class);

    @Test
    public void zeroToMax() {
        byte[] zero = new byte[] {(byte)0};
        byte[] max = new byte[] {(byte)255, (byte)255, (byte)255, (byte)255};

        logBytes("zeroToMax:zero", zero);
        logBytes("zeroToMax:max", max);

        Assert.assertEquals(1, byteArrayComparator.compare(zero, max));

        String unsignedZero = bytesToStr(zero);
        String unsignedMax = bytesToStr(max);

        Assert.assertEquals("00", unsignedZero);
        Assert.assertEquals("ffffffff", unsignedMax);

        LOG.debug("zeroToMax:unsignedZero " + unsignedZero);
        LOG.debug("zeroToMax:unsignedMax " + unsignedMax);

        Assert.assertTrue(unsignedZero.compareTo(unsignedMax) < 0);

        byte[] zeroAgain = strToBytes(unsignedZero);
        byte[] maxAgain = strToBytes(unsignedMax);

        Assert.assertArrayEquals(zero, zeroAgain);
        Assert.assertArrayEquals(max, maxAgain);
    }

    @Test
    public void positive() {
        byte[] lower = new byte[] {(byte)1, (byte)1};
        byte[] higher = new byte[] {(byte)1, (byte)2};

        logBytes("positive:lower", lower);
        logBytes("positive:higher", higher);

        Assert.assertEquals(-1, byteArrayComparator.compare(lower, higher));

        String unsignedLower = bytesToStr(lower);
        String unsignedHigher = bytesToStr(higher);

        Assert.assertEquals("0101", unsignedLower);
        Assert.assertEquals("0102", unsignedHigher);

        LOG.debug("positive:unsignedLower " + unsignedLower);
        LOG.debug("positive:unsignedHigher " + unsignedHigher);

        Assert.assertTrue(unsignedLower.compareTo(unsignedHigher) < 0);

        byte[] lowerAgain = strToBytes(unsignedLower);
        byte[] higherAgain = strToBytes(unsignedHigher);

        Assert.assertArrayEquals(lower, lowerAgain);
        Assert.assertArrayEquals(higher, higherAgain);
    }

    @Test
    public void negative() {
        byte[] lower = new byte[] {(byte)-2};
        byte[] higher = new byte[] {(byte)-1};

        logBytes("negative:lower", lower);
        logBytes("negative:higher", higher);

        Assert.assertEquals(-1, byteArrayComparator.compare(lower, higher));

        String unsignedLower = bytesToStr(lower);
        String unsignedHigher = bytesToStr(higher);

        Assert.assertEquals("fe", unsignedLower);
        Assert.assertEquals("ff", unsignedHigher);

        LOG.debug("negative:unsignedLower " + unsignedLower);
        LOG.debug("negative:unsignedHigher " + unsignedHigher);

        Assert.assertTrue(unsignedLower.compareTo(unsignedHigher) < 0);

        byte[] lowerAgain = strToBytes(unsignedLower);
        byte[] higherAgain = strToBytes(unsignedHigher);

        Assert.assertArrayEquals(lower, lowerAgain);
        Assert.assertArrayEquals(higher, higherAgain);
    }

    @Test
    public void aroundZero() {
        byte[] lower = new byte[] {(byte)-1};
        byte[] higher = new byte[] {(byte)1};

        logBytes("aroundZero:lower", lower);
        logBytes("aroundZero:higher", higher);

        Assert.assertEquals(-1, byteArrayComparator.compare(lower, higher));

        String unsignedLower = bytesToStr(lower);
        String unsignedHigher = bytesToStr(higher);

        Assert.assertEquals("ff", unsignedLower);
        Assert.assertEquals("01", unsignedHigher);

        LOG.debug("aroundZero:unsignedLower " + unsignedLower);
        LOG.debug("aroundZero:unsignedHigher " + unsignedHigher);

        Assert.assertTrue(unsignedHigher.compareTo(unsignedLower) < 0);

        byte[] lowerAgain = strToBytes(unsignedLower);
        byte[] higherAgain = strToBytes(unsignedHigher);

        Assert.assertArrayEquals(lower, lowerAgain);
        Assert.assertArrayEquals(higher, higherAgain);
    }

    @Test
    public void randomBiggerValue() {
        byte[] signed = new byte[] {(byte)0x81, (byte)0x80, (byte)0x7f};

        logBytes("random:signed", signed);

        String unsigned = bytesToStr(signed);

        LOG.debug("random:unsigned " + unsigned);

        Assert.assertEquals("81807f", unsigned);

        byte[] signedAgain = strToBytes(unsigned);

        Assert.assertArrayEquals(signed, signedAgain);
    }

    @Test
    public void limitAndOffset() {
        byte[] signed = new byte[]{(byte)0x1, (byte)0x2, (byte)0x3, (byte)0x4};

        String unsigned = bytesToStr(signed, 1, 3);
        Assert.assertEquals("0203", unsigned);
    }

    @Test
    public void basicMinus1() {
        byte[] bytes = new byte[] {(byte)0x23, (byte)0x2};
        Assert.assertEquals("2301", bytesToStrMinus1(bytes));
    }

    @Test
    public void minus1Borrow() {
        byte[] bytes = new byte[] {(byte)0x23, (byte)0x0};
        Assert.assertEquals("22ff", bytesToStrMinus1(bytes));
    }

    @Test
    public void minus1Zero() {
        byte[] bytes = new byte[] {(byte)0x0, (byte)0x0};
        Assert.assertEquals("00", bytesToStrMinus1(bytes));
    }

    @Test
    public void minus1CarryInMiddle() {
        byte[] bytes = new byte[] {(byte)0x9b, (byte)0x29, (byte)0x0};
        Assert.assertEquals("9b28ff", bytesToStrMinus1(bytes));
    }

    @Test
    public void minus1Offset() {
        byte[] bytes = new byte[] {(byte)0x3e, (byte)0x9b, (byte)0x29, (byte)0x0};
        Assert.assertEquals("9b28", bytesToStrMinus1(bytes, 1, 3));
    }


    private static Comparator<byte[]> byteArrayComparator = (o1, o2) -> {
        int length = Math.min(o1.length, o2.length);
        for (int i = 0; i < length; i++) {
            if (o1[i] < o2[i]) return -1;
            if (o1[i] > o2[i]) return 1;
        }
        if (o1.length > length) return 1;
        if (o2.length > length) return -1;
        return 0;
    };



}
