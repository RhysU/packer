package com.example.onnx.java;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.DoubleBuffer;

final class TestPacker {

    /** Columns available for specific test cases. */
    private enum Column { A, B, C, D }

    @Test
    void testSingle() {
        // Allocate storage for one single column
        final Packer<Column>.DoubleStorage s =
                new Packer<>(Column.A).new DoubleStorage(4);
        Assertions.assertEquals(4, s.capacity());

        // Fill the storage
        Assertions.assertTrue(s.hasRemaining());
        s.put(Column.A, 555);
        Assertions.assertTrue(s.hasRemaining());
        s.put(Column.A, 666);
        Assertions.assertTrue(s.hasRemaining());
        s.put(Column.A, 777);
        Assertions.assertTrue(s.hasRemaining());
        s.put(Column.A, 888);
        Assertions.assertFalse(s.hasRemaining());

        // Confirm all rows as expected
        final DoubleBuffer b = s.discard(0, 0);
        Assertions.assertEquals(0, b.position());
        Assertions.assertEquals(4, b.limit());
        Assertions.assertEquals(4, b.capacity());
        Assertions.assertEquals(555.0, b.get());
        Assertions.assertEquals(666.0, b.get());
        Assertions.assertEquals(777.0, b.get());
        Assertions.assertEquals(888.0, b.get());
    }
}
