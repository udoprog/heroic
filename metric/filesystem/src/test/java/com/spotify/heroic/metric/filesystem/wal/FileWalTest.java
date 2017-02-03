package com.spotify.heroic.metric.filesystem.wal;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class FileWalTest {
    @Test
    public void testNameDecodeBytes() {
        assertEquals(0x10L, decode(0x10));
        assertEquals(0x2010L, decode(0x20, 0x10));
        assertEquals(0x302010L, decode(0x30, 0x20, 0x10));
        assertEquals(0x40302010L, decode(0x40, 0x30, 0x20, 0x10));
        assertEquals(0x5040302010L, decode(0x50, 0x40, 0x30, 0x20, 0x10));
        assertEquals(0x605040302010L, decode(0x60, 0x50, 0x40, 0x30, 0x20, 0x10));
        assertEquals(0x70605040302010L, decode(0x70, 0x60, 0x50, 0x40, 0x30, 0x20, 0x10));
        assertEquals(0x8070605040302010L, decode(0x80, 0x70, 0x60, 0x50, 0x40, 0x30, 0x20, 0x10));
    }

    @Test
    public void testDecodeTxId() {
        assertEquals(0x10L, decodeTxId("10"));
        assertEquals(0x2010L, decodeTxId("2010"));
        assertEquals(0x302010L, decodeTxId("302010"));
        assertEquals(0x40302010L, decodeTxId("40302010"));
        assertEquals(0x5040302010L, decodeTxId("5040302010"));
        assertEquals(0x605040302010L, decodeTxId("605040302010"));
        assertEquals(0x70605040302010L, decodeTxId("70605040302010"));
        assertEquals(0x8070605040302010L, decodeTxId("8070605040302010"));
        assertEquals(0xaaaaL, decodeTxId("aaaa"));
        assertEquals(0xAAAAL, decodeTxId("AAAA"));
    }

    private long decodeTxId(final String name) {
        return FileWal.decodeTxId(FileWal.LOG_PREFIX + name);
    }

    private long decode(final int... ints) {
        final byte[] bytes = new byte[ints.length];

        for (int i = 0; i < ints.length; i++) {
            bytes[i] = (byte) (ints[i] & 0xff);
        }

        return FileWal.decodeNameBytes(bytes);
    }
}
