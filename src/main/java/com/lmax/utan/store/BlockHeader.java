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
        return lengthInBits(readHeader());
    }

    private static int lengthInBits(long headerValue)
    {
        return (int) (BIT_LENGTH_MASK & (headerValue >>> 32L));
    }

    public boolean isFrozen()
    {
        return isFrozen(readHeader());
    }

    private static boolean isFrozen(long headerValue)
    {
        return Long.highestOneBit(headerValue) == FROZEN_BIT;
    }

    public int lastTimestampDelta()
    {
        return lastTimestampDelta(readHeader());
    }

    private static int lastTimestampDelta(long headerValue)
    {
        return (int) (headerValue & 0xFFFFFFFFL);
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
        writeHeader(header);
    }

    void writeHeader(long headerValue)
    {
        buffer.putLongOrdered(0, headerValue);
    }

    static long widen(int value)
    {
        return ((long) value) & 0xFFFFFFFFL;
    }
}
