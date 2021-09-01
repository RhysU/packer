package com.example.onnx.java;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * Row-major storage of (rows, columns) with columns per a subset of an Enum.
 **/
public final class Packer<E extends Enum<E>> {

    private final E[] cols;         // Columns in each row in storage order
    private final int[] offs;       // Ordinal-based lookup of column offsets
    private final EnumSet<E> need;  // Columns required to be in each row

    /** Packer tracks configuration reusable across many buffers. */
    public Packer(final LinkedHashSet<E> columns) {
        // Store columns as an array to allow garbage-less iteration
        // Sanity checks that, at runtime, columns was well-formed
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("Empty columns");
        }
        final Iterator<E> iter = columns.iterator();
        final Class<E> enumClass = iter.next().getDeclaringClass();
        while (iter.hasNext()) {
            if (!enumClass.equals(iter.next().getDeclaringClass())) {
                throw new IllegalArgumentException("columns has mixed type");
            }
        }
        @SuppressWarnings("unchecked")
        final E[] storage = (E[]) Array.newInstance(enumClass, columns.size());
        cols = columns.toArray(storage);

        // Construct dense Enum -> column index mapping with unused sentry
        offs = new int[enumClass.getEnumConstants().length];
        Arrays.fill(offs, Integer.MIN_VALUE);
        for (int i = 0; i < cols.length; ++i) {
            offs[cols[i].ordinal()] = i;
        }

        // An additional EnumSet assists in tracking not-yet-supplied columns
        need = EnumSet.noneOf(enumClass);
        for (final E column : cols) {
            need.add(column);
        }
    }

    /** Convenience constructor requiring unique columns as varargs. */
    public Packer(final E... columns) {
        this(new LinkedHashSet<>(List.of(columns)));
        if (cols.length != columns.length) {
            throw new IllegalArgumentException("Duplicate(s) in columns");
        }
    }

    /** Use outer Packer configuration to pack doubles into direct buffers. */
    public final class DoubleStorage {

        private final DoubleBuffer w;  // Written in row-major order
        private final DoubleBuffer r;  // Read-only for exposure to user
        private final DoubleBuffer v;  // Independent position/limit/etc.
        private final EnumSet<E> m;    // Columns not in the present row

        /** Store up to the given number of rows in native ByteOrder. */
        public DoubleStorage(final int rows) {
            this(rows, ByteOrder.nativeOrder());
        }

        /** Store up to the given number of rows in supplied byteOrder. */
        public DoubleStorage(final int rows, final ByteOrder byteOrder) {
            // Storage is (a) direct and (b) zero-filled per DoubleBuffer API
            if (rows < 0) {
                throw new IllegalArgumentException("Negative rows " + rows);
            }
            final int doubles = Math.multiplyExact(rows, cols.length);
            final int bytes = Math.multiplyExact(Double.BYTES, doubles);
            w = ByteBuffer.allocateDirect(bytes)
                    .order(byteOrder)
                    .asDoubleBuffer();
            r = w.asReadOnlyBuffer();
            v = r.duplicate();
            if (!w.isDirect() || !r.isDirect() || !w.isDirect()) {
                throw new IllegalStateException("Indirect buffer(s)");
            }

            // Establish initial read/write state
            r.position(0).limit(0);         // No rows available for reading
            w.position(0).limit(doubles);   // All rows available for writing
            m = EnumSet.copyOf(need);       // All columns needed in row 0
        }

        /** Returns this buffer's capacity measured in rows. */
        public int capacity() {
            return r.capacity() / cols.length;
        }

        /** Do any rows exist between the current position and the limit? */
        public boolean hasRemaining() {
            return r.limit() != r.capacity();
        }

        /** Forget which columns are missing from the active row. */
        private void forget() {
            for (final E col : cols) m.add(col);  // No garbage
        }

        /**
         * Relative write of one column to the active row.
         * Value may be ignored if the given column is not stored.
         */
        public void put(final E column, final double value) {
            final int offset = offs[column.ordinal()];
            if (offset < 0) {
                return;
            }
            final int initial = r.limit();
            w.put(initial + offset, value);
            m.remove(column);
            if (m.isEmpty()) {
                r.limit(initial + cols.length);
                forget();
            }
        }

        /**
         * Relative write of multiple columns to the active row.
         * Values may be ignored for columns that are not stored.
         * Throws NullPointerException if any stored value is null.
         */
        public void put(final Map<E, ? extends Number> values) {
            for (final E column : cols) {
                put(column, values.get(column).doubleValue());
            }
        }

        /** Interface through which DoubleStorage can be retrieved. */
        public interface Observer<E> {
            void observe(int row, E Column, double value);
        }

        /** Absolute read of all columns in rows [begin, end). */
        public void get(
                final Observer<E> observer,
                final int begin,
                final int end)
        {
            r.position(begin * cols.length);
            for (int row = begin; row < end; ++row) {
                for (final E column : cols) {
                    observer.observe(row, column, r.get());
                }
            }
        }

        /**
         * Discard the initial headRows and the final tailRows of data.
         * Either headRows or tailRows may be zero.
         * Any partially written row is forgotten after a call to discard().
         *
         * All invocations of discard(...) return the *same* DoubleBuffer.
         * The caller may use discard(...).duplicate() if cloning desired.
         * The caller may use discard(0, 0) to simply obtain a view.
         *
         * @return a read-only view of stored values for low-level access.
         * The position is set to 0 and the limit reflects all remaining data.
         */
        public DoubleBuffer discard(final int headRows, final int tailRows) {
            if (headRows < 0) {
                throw new IllegalArgumentException("Negative headRows");
            }
            if (tailRows < 0) {
                throw new IllegalArgumentException("Negative tailRows");
            }

            // Drop tailRows by adjusting the r buffer limit
            final int tailDoubles = Math.multiplyExact(tailRows, cols.length);
            r.limit(Math.max(0, Math.subtractExact(r.limit(), tailDoubles)));

            // Drop headRows by via calling compact on write buffer
            final int headDoubles = Math.multiplyExact(headRows, cols.length);
            final int available = r.limit();
            w.position(Math.min(available, headDoubles)).limit(available);
            w.compact();

            // Re-establish state invariants and make put() begin a fresh row
            r.position(0).limit(w.position());
            w.position(0).limit(w.capacity());
            forget();

            // The view's position and limit reflect available data
            return v.position(0).limit(r.limit());
        }
    }
}