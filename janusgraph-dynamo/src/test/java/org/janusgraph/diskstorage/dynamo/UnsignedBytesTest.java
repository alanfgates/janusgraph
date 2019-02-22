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

import static org.janusgraph.diskstorage.dynamo.Utils.bytesToUnsignedBytes;
import static org.janusgraph.diskstorage.dynamo.Utils.unsignedBytesToBytes;

public class UnsignedBytesTest {
    private static final Logger LOG = LoggerFactory.getLogger(UnsignedBytesTest.class);

    @Test
    public void zeroToMax() {
        byte[] zero = new byte[] {(byte)0};
        byte[] max = new byte[] {(byte)255, (byte)255, (byte)255, (byte)255};

        logBytes("zeroToMax:zero", zero);
        logBytes("zeroToMax:max", max);

        Assert.assertEquals(1, unsignedComparator.compare(zero, max));

        byte[] unsignedZero = bytesToUnsignedBytes(zero, 0, zero.length);
        byte[] unsignedMax = bytesToUnsignedBytes(max, 0, max.length);

        logBytes("zeroToMax:unsignedZero", unsignedZero);
        logBytes("zeroToMax:unsignedMax", unsignedMax);

        Assert.assertEquals(-1, unsignedComparator.compare(unsignedZero, unsignedMax));

        byte[] zeroAgain = unsignedBytesToBytes(unsignedZero);
        byte[] maxAgain = unsignedBytesToBytes(unsignedMax);

        Assert.assertArrayEquals(zero, zeroAgain);
        Assert.assertArrayEquals(max, maxAgain);
    }

    @Test
    public void positive() {
        byte[] lower = new byte[] {(byte)1, (byte)1};
        byte[] higher = new byte[] {(byte)1, (byte)2};

        logBytes("positive:lower", lower);
        logBytes("positive:higher", higher);

        Assert.assertEquals(-1, unsignedComparator.compare(lower, higher));

        byte[] unsignedLower = bytesToUnsignedBytes(lower, 0, lower.length);
        byte[] unsignedHigher = bytesToUnsignedBytes(higher, 0, higher.length);

        logBytes("positive:unsignedLower", unsignedLower);
        logBytes("positive:unsignedHigher", unsignedHigher);

        Assert.assertEquals(-1, unsignedComparator.compare(unsignedLower, unsignedHigher));

        byte[] lowerAgain = unsignedBytesToBytes(unsignedLower);
        byte[] higherAgain = unsignedBytesToBytes(unsignedHigher);

        Assert.assertArrayEquals(lower, lowerAgain);
        Assert.assertArrayEquals(higher, higherAgain);
    }

    @Test
    public void negative() {
        byte[] lower = new byte[] {(byte)-2};
        byte[] higher = new byte[] {(byte)-1};

        logBytes("negative:lower", lower);
        logBytes("negative:higher", higher);

        Assert.assertEquals(-1, unsignedComparator.compare(lower, higher));

        byte[] unsignedLower = bytesToUnsignedBytes(lower, 0, lower.length);
        byte[] unsignedHigher = bytesToUnsignedBytes(higher, 0, higher.length);

        logBytes("negative:unsignedLower", unsignedLower);
        logBytes("negative:unsignedHigher", unsignedHigher);

        Assert.assertEquals(-1, unsignedComparator.compare(unsignedLower, unsignedHigher));

        byte[] lowerAgain = unsignedBytesToBytes(unsignedLower);
        byte[] higherAgain = unsignedBytesToBytes(unsignedHigher);

        Assert.assertArrayEquals(lower, lowerAgain);
        Assert.assertArrayEquals(higher, higherAgain);
    }

    @Test
    public void aroundZero() {
        byte[] lower = new byte[] {(byte)-1};
        byte[] higher = new byte[] {(byte)1};

        logBytes("aroundZero:lower", lower);
        logBytes("aroundZero:higher", higher);

        Assert.assertEquals(-1, unsignedComparator.compare(lower, higher));

        byte[] unsignedLower = bytesToUnsignedBytes(lower, 0, lower.length);
        byte[] unsignedHigher = bytesToUnsignedBytes(higher, 0, higher.length);

        logBytes("aroundZero:unsignedLower", unsignedLower);
        logBytes("aroundZero:unsignedHigher", unsignedHigher);

        Assert.assertEquals(1, unsignedComparator.compare(unsignedLower, unsignedHigher));

        byte[] lowerAgain = unsignedBytesToBytes(unsignedLower);
        byte[] higherAgain = unsignedBytesToBytes(unsignedHigher);

        Assert.assertArrayEquals(lower, lowerAgain);
        Assert.assertArrayEquals(higher, higherAgain);
    }

    @Test
    public void around7f() {
        byte[] lowest = new byte[] {(byte)0x81};
        byte[] lower = new byte[] {(byte)0x80};
        byte[] higher = new byte[] {(byte)0x7f};

        logBytes("around7f:lower", lower);
        logBytes("around7f:higher", higher);

        Assert.assertEquals(-1, unsignedComparator.compare(lower, higher));
        Assert.assertEquals(1, unsignedComparator.compare(lowest, lower));

        byte[] unsignedLowest = bytesToUnsignedBytes(lowest, 0, lower.length);
        byte[] unsignedLower = bytesToUnsignedBytes(lower, 0, lower.length);
        byte[] unsignedHigher = bytesToUnsignedBytes(higher, 0, higher.length);

        logBytes("around7f:unsignedLower", unsignedLower);
        logBytes("around7f:unsignedHigher", unsignedHigher);

        Assert.assertEquals(1, unsignedComparator.compare(unsignedLower, unsignedHigher));
        Assert.assertEquals(1, unsignedComparator.compare(unsignedLowest, unsignedLower));

        byte[] lowerAgain = unsignedBytesToBytes(unsignedLower);
        byte[] higherAgain = unsignedBytesToBytes(unsignedHigher);
        byte[] lowestAgain = unsignedBytesToBytes(unsignedLowest);

        Assert.assertArrayEquals(lower, lowerAgain);
        Assert.assertArrayEquals(higher, higherAgain);
        Assert.assertArrayEquals(lowest, unsignedLowest);
    }

    private static Comparator<byte[]> unsignedComparator = (o1, o2) -> {
        int length = Math.min(o1.length, o2.length);
        for (int i = 0; i < length; i++) {
            if (o1[i] < o2[i]) return -1;
            if (o1[i] > o2[i]) return 1;
        }
        if (o1.length > length) return 1;
        if (o2.length > length) return -1;
        return 0;
    };

    private void logBytes(String title, byte[] bytes) {
        StringBuilder buf = new StringBuilder(title);
        for (byte b : bytes) buf.append(' ').append(b);
        LOG.debug(buf.toString());
    }



}
