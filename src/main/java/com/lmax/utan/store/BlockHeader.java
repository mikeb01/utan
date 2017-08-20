package com.lmax.utan.store;

import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

import static com.lmax.utan.store.Block.BYTE_ORDER;
import static com.lmax.utan.store.Block.FIRST_TIMESTAMP_OFFSET;

public class BlockHeader
{
    @SuppressWarnings("NumericOverflow")
    private static final long FROZEN_BIT = 1L << 63;
    private static final long BIT_LENGTH_MASK = 0x7FFFFFFFL;

    private final AtomicBuffer buffer;

    public BlockHeader(AtomicBuffer buffer)
    {
        this.buffer = buffer;
    }

    public static BlockHeader allocateDirect()
    {
        return new BlockHeader(new UnsafeBuffer(ByteBuffer.allocateDirect(16)));
    }

    public ByteBuffer underlyingBuffer()
    {
        ByteBuffer byteBuffer = buffer.byteBuffer();
        return byteBuffer == null ? ByteBuffer.wrap(buffer.byteArray()) : byteBuffer;
    }

    long readHeader()
    {
        return buffer.getLongVolatile(0);
    }

    public int lengthInBits()
    {
        return (int) (BIT_LENGTH_MASK & (readHeader() >>> 32L));
    }

    public boolean isFrozen()
    {
        return Long.highestOneBit(readHeader()) == FROZEN_BIT;
    }

    public long firstTimestamp()
    {
        return buffer.getLong(FIRST_TIMESTAMP_OFFSET, BYTE_ORDER);
    }

    public long lastTimestamp()
    {
        return firstTimestamp() + lastTimestampDelta();
    }

    void writeHeader(boolean isFrozen, int length, int lastTimestampDelta)
    {
        long frozenBit = isFrozen ? FROZEN_BIT : 0;
        long header = frozenBit | widen(length) << 32L | widen(lastTimestampDelta);
        buffer.putLongOrdered(0, header);
    }

    int lastTimestampDelta()
    {
        return (int) (readHeader() & 0xFFFFFFFFL);
    }

    static long widen(int value)
    {
        return ((long) value) & 0xFFFFFFFFL;
    }
}
