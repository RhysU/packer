/*
 * Copyright (C) 2021 Rhys Ulerich <rhys.ulerich@gmail.com>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.example.onnx.java;

import org.junit.jupiter.api.Test;

import java.nio.DoubleBuffer;
import java.util.EnumMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class TestPacker {

    /** Columns available for specific test cases. */
    private enum Column { A, B, C, D }

    /** Storing one column works. */
    @Test
    void testOneColumn() {
        // Allocate storage for one single column
        final Packer<Column>.DoubleStorage s =
                new Packer<>(Column.B).new DoubleStorage(4);
        assertEquals(4, s.capacity());

        // Fill the storage
        assertTrue(s.hasRemaining());
        s.pack(Column.B, 555);
        assertTrue(s.hasRemaining());
        s.pack(Column.A, 12);  // Ignored
        s.pack(Column.B, 666);
        s.pack(Column.C, 34);  // Ignored
        s.pack(Column.D, 56);  // Ignored
        assertTrue(s.hasRemaining());
        // A and D ignored while C not supplied
        s.pack(new EnumMap<>(Map.of(Column.A, 1, Column.B, 777, Column.D, 2)));
        assertTrue(s.hasRemaining());
        s.pack(Column.B, 888);
        assertFalse(s.hasRemaining());

        // Confirm all rows as expected
        final DoubleBuffer b1 = s.discard(0, 0);
        assertEquals(0, b1.position());
        assertEquals(4, b1.limit());
        assertEquals(4, b1.capacity());
        assertTrue(b1.hasRemaining());
        assertEquals(555.0, b1.get());
        assertEquals(666.0, b1.get());
        assertEquals(777.0, b1.get());
        assertEquals(888.0, b1.get());
        assertFalse(b1.hasRemaining());

        // Discard the last row and reconfirm expectations
        final DoubleBuffer b2 = s.discard(0, 1);
        assertEquals(0, b2.position());
        assertEquals(3, b2.limit());
        assertEquals(4, b2.capacity());
        assertTrue(b2.hasRemaining());
        assertEquals(555.0, b2.get());
        assertEquals(666.0, b2.get());
        assertEquals(777.0, b2.get());
        assertFalse(b2.hasRemaining());
        assertSame(b1, b2, "discard(...) reuses");

        // Discard the first two rows and reconfirm expectations
        final DoubleBuffer b3 = s.discard(2, 0);
        assertEquals(0, b3.position());
        assertEquals(1, b3.limit());
        assertEquals(4, b3.capacity());
        assertTrue(b3.hasRemaining());
        assertEquals(777.0, b3.get());
        assertFalse(b3.hasRemaining());
        assertSame(b1, b3, "discard(...) reuses");

        // Discard more than remaining rows and reconfirm expectations
        final DoubleBuffer b4 = s.discard(9, 9);
        assertEquals(0, b4.position());
        assertEquals(0, b4.limit());
        assertEquals(4, b4.capacity());
        assertFalse(b4.hasRemaining());
        assertSame(b1, b4, "discard(...) reuses");
    }

    /** Storing two columns works. */
    @Test
    void testTwoColumns() {
        // Allocate storage for two columns in non-ordinal order
        final Packer<Column>.DoubleStorage s =
                new Packer<>(Column.B, Column.A).new DoubleStorage(4);
        assertEquals(4, s.capacity());

        // Fill the storage
        assertTrue(s.hasRemaining());
        s.pack(Column.B, 555);   // Supplied in correct order
        s.pack(Column.A, 1);
        s.pack(Column.C, 1234);  // Ignored
        assertTrue(s.hasRemaining());
        s.pack(Column.A, 2);  // Supplied in reversed order
        s.pack(Column.B, 666);
        assertTrue(s.hasRemaining());
        // D ignored while C not supplied
        s.pack(new EnumMap<>(Map.of(Column.A, 3, Column.B, 777, Column.D, 2)));
        assertTrue(s.hasRemaining());
        s.pack(Column.B, 888);
        assertTrue(s.hasRemaining());  // !hasRemaining until row complete
        s.pack(Column.A, 4);
        assertFalse(s.hasRemaining());

        // Confirm all rows as expected
        final DoubleBuffer b1 = s.discard(0, 0);
        assertEquals(0, b1.position());
        assertEquals(8, b1.limit());
        assertEquals(8, b1.capacity());
        assertTrue(b1.hasRemaining());
        assertEquals(555.0, b1.get());
        assertEquals(1.0, b1.get());
        assertEquals(666.0, b1.get());
        assertEquals(2.0, b1.get());
        assertEquals(777.0, b1.get());
        assertEquals(3.0, b1.get());
        assertEquals(888.0, b1.get());
        assertEquals(4.0, b1.get());
        assertFalse(b1.hasRemaining());

        // Discard the last row and reconfirm expectations
        final DoubleBuffer b2 = s.discard(0, 1);
        assertEquals(0, b2.position());
        assertEquals(6, b2.limit());
        assertEquals(8, b2.capacity());
        assertTrue(b2.hasRemaining());
        assertEquals(555.0, b2.get());
        assertEquals(1.0, b2.get());
        assertEquals(666.0, b2.get());
        assertEquals(2.0, b2.get());
        assertEquals(777.0, b2.get());
        assertEquals(3.0, b2.get());
        assertFalse(b2.hasRemaining());
        assertSame(b1, b2, "discard(...) reuses");

        // Discard the first two rows and reconfirm expectations
        final DoubleBuffer b3 = s.discard(2, 0);
        assertEquals(0, b3.position());
        assertEquals(2, b3.limit());
        assertEquals(8, b3.capacity());
        assertTrue(b3.hasRemaining());
        assertEquals(777.0, b3.get());
        assertEquals(3.0, b3.get());
        assertFalse(b3.hasRemaining());
        assertSame(b1, b3, "discard(...) reuses");

        // Discard more than remaining rows and reconfirm expectations
        final DoubleBuffer b4 = s.discard(9, 9);
        assertEquals(0, b4.position());
        assertEquals(0, b4.limit());
        assertEquals(8, b4.capacity());
        assertFalse(b4.hasRemaining());
        assertSame(b1, b4, "discard(...) reuses");
    }
}
