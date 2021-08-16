package com.example.onnx.java;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;

import static java.lang.Math.addExact;
import static java.lang.Math.negateExact;
import static java.lang.Math.subtractExact;

/**
 * Row-major storage of (rows, columns) with columns per a subset of an Enum.
 **/
public final class DoublePacker<E extends Enum<E>> {

    private final DoubleBuffer db;  // Written in row-major order
    private final DoubleBuffer ro;  // Read-only for exposure to user
    private final E[] cols;         // Columns in each row in storage order
    private final int[] offs;       // Column offsets relative to db.limit()
    private final EnumSet<E> requireCols;
    private final EnumSet<E> missingCols;

    public DoublePacker(
            final ByteOrder byteOrder,
            final int rows,
            final LinkedHashSet<E> columns
    ) {
        // Storage is (a) direct and (b) zero-initialized per DoubleBuffer API
        if (rows < 0) {
            throw new IllegalArgumentException("Negative rows " + rows);
        }
        final int doubles = Math.multiplyExact(rows, columns.size());
        final int bytes = Math.multiplyExact(Double.BYTES, doubles);
        db = ByteBuffer.allocateDirect(bytes)
                .order(byteOrder)
                .asDoubleBuffer();
        if (!db.isDirect()) {
            throw new IllegalStateException("Indirect buffer");
        }
        ro = db.asReadOnlyBuffer();

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

        // Construct dense Enum -> column index mapping that breaks on misuse
        // First, compute zero-indexed dense ordinals
        offs = new int[enumClass.getEnumConstants().length];
        Arrays.fill(offs, negateExact(addExact(db.capacity(), 1)));
        for (int i = 0; i < cols.length; ++i) {
            offs[cols[i].ordinal()] = i;
        }
        // Second, adjust to be relative to position 1 past the end of row
        for (int j = 0; j < offs.length; ++j) {
            offs[j] = negateExact(subtractExact(cols.length, offs[j]));
        }

        // Each row must contain all required columns
        requireCols = EnumSet.copyOf(columns);

        // Track first row which presently contains no required columns
        db.limit(cols.length);
        missingCols = EnumSet.copyOf(requireCols);
    }

    /**
     * How many rows can be stored?
     */
    public int capacity() {
        return db.capacity() / cols.length;
    }

    /**
     * How many complete rows are available for consumption via get()?
     */
    public int available() {
        return (db.limit() / cols.length) - (missingCols.isEmpty() ? 0 : 1);
    }

    /**
     * Has all capacity been used?
     */
    public boolean isFull() {
        return db.limit() == db.capacity() && missingCols.isEmpty();
    }

    /**
     * Advance to the next row so that data can be put() into it.
     */
    public void advance() {
        if (!missingCols.isEmpty()) {
            throw new IllegalStateException("Missing columns " + missingCols);
        }
        final int oldLimit = db.limit();
        if (oldLimit == db.capacity()) {
            throw new IllegalStateException("Beyond capacity " + capacity());
        }
        db.limit(oldLimit + cols.length);
        missingCols.addAll(requireCols);
    }

    /**
     * Put a value into the active row throwing IOOBE if column invalid.
     */
    public void put(final E column, final double value) {
        db.put(db.limit() + offs[column.ordinal()], value);
        missingCols.remove(column);
    }

    /**
     * Put required values into current row ignoring whenever value is null.
     */
    public void put(Map<E, Double> values) {
        db.position(db.limit() - cols.length);
        for (final E column : cols) {
            final Double value = values.get(column);
            if (value == null) {
                db.get();  // Advance
            } else {
                db.put(value);
                missingCols.remove(column);
            }
        }
    }

    public interface Observer<F extends Enum<F>> {
        void observe(int row, F column, double value);
    }

    /**
     * Get rows [beginIncl, endExcl) via some Observer.
     */
    public void get(
            final Observer<E> observer,
            final int beginIncl,
            final int endExcl)
    {
        if (beginIncl < 0) {
            throw new IllegalArgumentException("Negative beginIncl");
        }
        final int available = available();
        if (endExcl > available) {
            throw new IllegalStateException(
                    "endExcl more than available " + available);
        }
        db.position(beginIncl * cols.length);
        for (int row = beginIncl; row < endExcl; ++row) {
            for (E column : cols) {
                observer.observe(row, column, db.get());
            }
        }
    }

    /**
     * A read-only version of the backing buffer.
     */
    public DoubleBuffer buffer() {
        return ro;
    }

    /**
     * Discard some leading number of rows.
     **/
    public void discard(final int leadingRows) {
        // Misuse checks and simple cases
        if (leadingRows < 0) {
            throw new IllegalArgumentException("Negative leadingRows");
        }
        if (leadingRows == 0) {
            return;
        }
        final int available = available();
        if (leadingRows == available) {
            db.limit(cols.length);
            missingCols.addAll(requireCols);
            return;
        }
        if (leadingRows > available) {
            throw new IllegalArgumentException(
                    "More than available " + available);
        }

        // General case
        final int oldLimit = db.limit();
        db.position(leadingRows * cols.length);
        // db.limit() is ready for compaction
        db.compact();

        // Adjust currently tracked row
        final int subLimit = leadingRows - (missingCols.isEmpty() ? 1: 0);
        db.limit(oldLimit - subLimit * cols.length);
        missingCols.addAll(requireCols);  // Hence -1 above
    }

}