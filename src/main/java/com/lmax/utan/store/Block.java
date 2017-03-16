package com.lmax.utan.store;

import org.agrona.BitUtil;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;

import static java.lang.Long.highestOneBit;
import static java.lang.Long.numberOfTrailingZeros;
import static java.lang.String.format;
import static java.nio.ByteOrder.BIG_ENDIAN;

public class Block
{
    private static final int HEADER_LENGTH = 128;
    static final int COMPRESSED_DATA_START = HEADER_LENGTH + 128;
    private static final int FIRST_TIMESTAMP_OFFSET = HEADER_LENGTH / 8;
    private static final int FIRST_VALUE_OFFSET = FIRST_TIMESTAMP_OFFSET + 8;
    private static final int BYTE_LENGTH = 4096;
    private static final int BIT_LENGTH_LIMIT = BYTE_LENGTH * 8;
    private static final int INT_LENGTH = BYTE_LENGTH / 4;
    private static final int ALL_THE_LEASES = 1024;

    private final AtomicBuffer buffer;
    private final Semaphore resetSemaphore = new Semaphore(ALL_THE_LEASES);

    private long tMinusOne = 0;
    private long tMinusTwo = 0;
    private double lastValue = 0.0;
    private long lastXorValue = 0;

    private int temp0 = 0;
    private int temp1 = 0;
    private int temp2 = 0;
    private int temp3 = 0;

    public Block()
    {
        this(new UnsafeBuffer(new byte[4096]));
    }

    public Block(AtomicBuffer buffer)
    {
        this.buffer = buffer;
        reset();
    }

    public static Block new4kHeapBlock()
    {
        return new Block(new UnsafeBuffer(new byte[4096]));
    }

    public static Block[] new4KDirectBlocks(int n)
    {
        final int size = n * BYTE_LENGTH;
        final ByteBuffer backingBuffer = ByteBuffer.allocateDirect(size);
        final Block[] blocks = new Block[n];

        for (int i = 0; i < n; i++)
        {
            UnsafeBuffer blockBuffer = new UnsafeBuffer(backingBuffer, i * BYTE_LENGTH, BYTE_LENGTH);
            blocks[i] = new Block(blockBuffer);
        }

        return blocks;
    }

    private void setLengthInBits(int length)
    {
        buffer.putIntOrdered(0, length);
    }

    public int lengthInBits()
    {
        return buffer.getIntVolatile(0);
    }

    // ====================
    // Writing to the block
    // ====================

    public synchronized boolean append(long timestamp, double val)
    {
        int bitOffset = lengthInBits();
        if (bitOffset == HEADER_LENGTH)
        {
            appendInitial(timestamp, val);
            return true;
        }
        else
        {
            return appendCompressed(bitOffset, timestamp, val);
        }
    }

    public boolean isEmpty()
    {
        return lengthInBits() == HEADER_LENGTH;
    }

    public long firstTimestamp()
    {
        return buffer.getLong(FIRST_TIMESTAMP_OFFSET, BIG_ENDIAN);
    }

    private void appendInitial(long timestamp, double val)
    {
        buffer.putLong(FIRST_TIMESTAMP_OFFSET, timestamp, BIG_ENDIAN);
        buffer.putDouble(FIRST_VALUE_OFFSET, val, BIG_ENDIAN);
        setLengthInBits(COMPRESSED_DATA_START);

        tMinusOne = timestamp;
        tMinusTwo = timestamp;
        lastValue = val;
    }

    private boolean appendCompressed(int bufferBitIndex, long timestamp, double val)
    {
        resetBitBuffer();

        final long d = (timestamp - tMinusOne) - (tMinusOne - tMinusTwo);

        final int timestampBitsAdded;
        if (d == 0)
        {
            timestampBitsAdded = appendZeroTimestampDelta();
        }
        else if (-64 <= d && d <= 63)
        {
            timestampBitsAdded = appendTimestampDelta(0, 7, 0b10, d);
        }
        else if (-256 <= d && d <= 255)
        {
            timestampBitsAdded = appendTimestampDelta(0, 9, 0b110, d);
        }
        else if (-2048 <= d && d <= 2047)
        {
            timestampBitsAdded = appendTimestampDelta(0, 12, 0b1110, d);
        }
        else if (Integer.MIN_VALUE <= d && d <= Integer.MAX_VALUE)
        {
            timestampBitsAdded = appendTimestampDelta(0, 32, 0b11110, d);
        }
        else
        {
            throw new IllegalArgumentException(format(
                "Timestamp delta out of range - delta: %d, timestamp: %d, tMinusOne: %d, tMinusTwo: %d",
                d, timestamp, tMinusOne, tMinusTwo));
        }

        long valueAsLong = Double.doubleToLongBits(val);
        long lastValueAsLong = Double.doubleToLongBits(lastValue);
        long xorValue = valueAsLong ^ lastValueAsLong;

        final int valueBitsAdded;
        if (xorValue == 0)
        {
            valueBitsAdded = appendZeroValueXor(timestampBitsAdded);
        }
        else
        {
            valueBitsAdded = appendValueXor(timestampBitsAdded, xorValue, lastXorValue);
        }

        int totalBitsAdded = timestampBitsAdded + valueBitsAdded;

        final int newBitLength = bufferBitIndex + totalBitsAdded;

        if (newBitLength > BIT_LENGTH_LIMIT)
        {
            return false;
        }

        flushTemp(bufferBitIndex, totalBitsAdded);

        tMinusTwo = tMinusOne;
        tMinusOne = timestamp;
        lastValue = val;
        lastXorValue = xorValue;

        setLengthInBits(newBitLength);

        return true;
    }

    private int appendZeroTimestampDelta()
    {
        return 1;
    }

    private int appendTimestampDelta(int bitOffset, int numBits, int markerBits, long timestampDelta)
    {
        int markerBitLength = numberOfTrailingZeros(highestOneBit(markerBits) << 1);

        writeBits(bitOffset, (long) markerBits, markerBitLength);
        writeBits(bitOffset + markerBitLength, compressBits(timestampDelta, numBits), numBits);

        return markerBitLength + numBits;
    }

    private int appendZeroValueXor(int i)
    {
        return 1;
    }

    private int appendValueXor(int bitOffset, long xorValue, long previousXorValue)
    {
        int leadingZeros = Long.numberOfLeadingZeros(xorValue);
        int trailingZeros = Long.numberOfTrailingZeros(xorValue);
        int prevLeadingZeros = Long.numberOfLeadingZeros(previousXorValue);
        int prevTrailingZeros = Long.numberOfTrailingZeros(previousXorValue);

        int length = 0;

        if (leadingZeros >= prevLeadingZeros && trailingZeros >= prevTrailingZeros)
        {
            length += writeBits(bitOffset, (long) 0b10, 2);
            length += writeBits(
                bitOffset + 2, xorValue >>> prevTrailingZeros, 64 - (prevLeadingZeros + prevTrailingZeros));
        }
        else
        {
            int relevantLength = 64 - (leadingZeros + trailingZeros);

            length += writeBits(bitOffset, 0b11L, 2);
            length += writeBits(bitOffset + 2, (long) leadingZeros, 5);
            length += writeBits(bitOffset + 2 + 5, (long) (relevantLength - 1), 6);
            length += writeBits(bitOffset + 2 + 5 + 6, xorValue >>> trailingZeros, relevantLength);
        }

        return length;
    }

    int writeBits(int tempBitIndex, long value, int valueBitLength)
    {
        assert valueInRange(valueBitLength, value) : format("value out of range - writeBits(%d, %d (%d), %d)", tempBitIndex, valueBitLength, 1L << valueBitLength, value);
        assert tempBitIndex + valueBitLength < 128 : format("value too long - writeBits(%d, %d (%d), %d)", tempBitIndex, valueBitLength, 1L << valueBitLength, value);

        final int tempIntIndex = tempBitIndex / 32;
        final int tempBitOffset = tempBitIndex % 32;

        long shiftedValue = value << 64 - valueBitLength;
        int upperPart = (int) (0xFFFFFFFFL & (shiftedValue >>> 32));
        int lowerPart = (int) (0xFFFFFFFFL & shiftedValue);

        final int localTemp0 = getTempPart(tempIntIndex) | upperPart >>> tempBitOffset;
        final int localTemp1 = (upperPart & intMask(tempBitOffset)) << (32 - tempBitIndex) | lowerPart >>> tempBitOffset;
        final int localTemp2 = (lowerPart & intMask(tempBitOffset)) << (32 - tempBitIndex);

        final int intsToWrite = (tempBitOffset + valueBitLength + 32 - 1) / 32;
        assert intsToWrite <= 3;

        switch (intsToWrite)
        {
            case 3:
                setTempPart(tempIntIndex + 2, localTemp2);
            case 2:
                setTempPart(tempIntIndex + 1, localTemp1);
            case 1:
                setTempPart(tempIntIndex, localTemp0);
            default:
                // Ignore
        }

        return valueBitLength;
    }

    private void setTempPart(int bitBufferPartIndex, int toAppend)
    {
        switch (bitBufferPartIndex)
        {
            case 0:
                this.temp0 |= toAppend;
                break;
            case 1:
                this.temp1 |= toAppend;
                break;
            case 2:
                this.temp2 |= toAppend;
                break;
            case 3:
                this.temp3 |= toAppend;
                break;
            default:
                assert false : "Invalid bit buffer part index: " + bitBufferPartIndex;
        }
    }

    int getTempPart(int bitBufferPartIndex)
    {
        switch (bitBufferPartIndex)
        {
            case 0:
                return this.temp0;
            case 1:
                return this.temp1;
            case 2:
                return this.temp2;
            case 3:
                return this.temp3;
        }

        throw new IllegalStateException(bitBufferPartIndex + "");
    }

    private void resetBitBuffer()
    {
        temp0 = 0;
        temp1 = 0;
        temp2 = 0;
        temp3 = 0;
    }

    private void flushTemp(int bufferBitIndex, int bitLength)
    {
        assert bitLength <= 128;

        int intAlignedBufferByteIndex = (bufferBitIndex / 32) * 4;
        int bufferBitSubIndex = bufferBitIndex & 31;

        int existingValue = buffer.getInt(intAlignedBufferByteIndex, BIG_ENDIAN);

        int tempShifted0 = existingValue | (temp0 >>> bufferBitSubIndex);
        int tempShifted1 = (temp0 & intMask(bufferBitSubIndex)) << (32 - bufferBitSubIndex) | (temp1 >>> bufferBitSubIndex);
        int tempShifted2 = (temp1 & intMask(bufferBitSubIndex)) << (32 - bufferBitSubIndex) | (temp2 >>> bufferBitSubIndex);
        int tempShifted3 = (temp2 & intMask(bufferBitSubIndex)) << (32 - bufferBitSubIndex) | (temp3 >>> bufferBitSubIndex);
        int tempShifted4 = (temp3 & intMask(bufferBitSubIndex)) << (32 - bufferBitSubIndex);

        final int intsToWrite = (bufferBitSubIndex + bitLength + 32 - 1) / 32;
        assert intsToWrite <= 5;

        switch (intsToWrite)
        {
            case 5:
                buffer.putInt(intAlignedBufferByteIndex + 16, tempShifted4, BIG_ENDIAN);
            case 4:
                buffer.putInt(intAlignedBufferByteIndex + 12, tempShifted3, BIG_ENDIAN);
            case 3:
                buffer.putInt(intAlignedBufferByteIndex + 8, tempShifted2, BIG_ENDIAN);
            case 2:
                buffer.putInt(intAlignedBufferByteIndex + 4, tempShifted1, BIG_ENDIAN);
            case 1:
                buffer.putInt(intAlignedBufferByteIndex, tempShifted0, BIG_ENDIAN);
            default:
                // Ignore
        }
    }

    // ======================
    // Iterating over results
    // ======================

    public int foreach(ValueConsumer consumer)
    {
        if (resetSemaphore.tryAcquire())
        {
            try
            {
                return doForEach(consumer);
            }
            finally
            {
                resetSemaphore.release();
            }
        }
        else
        {
            return 0;
        }
    }

    private int doForEach(ValueConsumer consumer)
    {
        int lengthInBits = lengthInBits();

        if (lengthInBits <= HEADER_LENGTH)
        {
            return 0;
        }

        long timestamp = buffer.getLong(FIRST_TIMESTAMP_OFFSET, BIG_ENDIAN);
        double value = buffer.getDouble(FIRST_VALUE_OFFSET, BIG_ENDIAN);
        long lastXorValue = 0;

        consumer.accept(timestamp, value);

        long tMinusOne = timestamp;
        long tMinusTwo = timestamp;

        int bitOffset = HEADER_LENGTH + ((BitUtil.SIZE_OF_LONG + BitUtil.SIZE_OF_LONG) * 8);

        int count = 1;
        while (bitOffset < lengthInBits)
        {
            final long delta;
            if (0 == readBits(bitOffset, 1))
            {
                bitOffset += 1;
                delta = 0;
            }
            else if (0b10 == readBits(bitOffset, 2))
            {
                bitOffset += 2;
                delta = decompressBits(readBits(bitOffset, 7), 7);
                bitOffset += 7;
            }
            else if (0b110 == readBits(bitOffset, 3))
            {
                bitOffset += 3;
                delta = decompressBits(readBits(bitOffset, 9), 9);
                bitOffset += 9;
            }
            else if (0b1110 == readBits(bitOffset, 4))
            {
                bitOffset += 4;
                delta = decompressBits(readBits(bitOffset, 12), 12);
                bitOffset += 12;
            }
            else if (0b11110 == readBits(bitOffset, 5))
            {
                bitOffset += 5;
                delta = decompressBits(readBits(bitOffset, 32), 32);
                bitOffset += 32;
            }
            else
            {
                throw new IllegalStateException("Data Corrupt");
            }

            timestamp =  delta + (tMinusOne - tMinusTwo) + tMinusOne;
            tMinusTwo = tMinusOne;
            tMinusOne = timestamp;

            if (0 == readBits(bitOffset, 1))
            {
                bitOffset += 1;
            }
            else
            {
                long prefixBits = readBits(bitOffset, 2);
                bitOffset += 2;

                if (0b10 == prefixBits)
                {
                    int leadingZeros = Long.numberOfLeadingZeros(lastXorValue);
                    int trailingZeros = Long.numberOfTrailingZeros(lastXorValue);
                    int validBits = 64 - (leadingZeros + trailingZeros);
                    long xorValue = readBitsLong(bitOffset, validBits) << trailingZeros;

                    value = Double.longBitsToDouble(xorValue ^ Double.doubleToLongBits(value));

                    lastXorValue = xorValue;

                    bitOffset += validBits;
                }
                else if (0b11 == prefixBits)
                {
                    int leadingZeros = readBits(bitOffset, 5);
                    int length = readBits(bitOffset + 5, 6) + 1;
                    long shift = 64 - (leadingZeros + length);
                    long xorValue = readBitsLong(bitOffset + 5 + 6, length) << shift;

                    value = Double.longBitsToDouble(xorValue ^ Double.doubleToLongBits(value));

                    lastXorValue = xorValue;

                    bitOffset += 5;
                    bitOffset += 6;
                    bitOffset += length;
                }
                else
                {
                    throw new IllegalStateException("Data Corrupt");
                }
            }

            consumer.accept(timestamp, value);
            count++;
        }

        return count;
    }

    private int readBits(int bitOffset, int numBits)
    {
        return (int) (readBitsLong(bitOffset, numBits) & 0xFFFFFFFFL);
    }

    private long readBitsLong(int bitOffset, int numBits)
    {
        int longAlignedByteOffset = (bitOffset / 64) * 8;
        int bitSubIndex = bitOffset & 63;

        long bitsUpper = buffer.getLong(longAlignedByteOffset, BIG_ENDIAN);
        long bitsLower;
        if (longAlignedByteOffset + 8 < buffer.capacity())
        {
            bitsLower = buffer.getLong(longAlignedByteOffset + 8, BIG_ENDIAN);
        }
        else
        {
            bitsLower = 0;
        }

        int shiftRight = (64 - bitSubIndex) - numBits;

        long mask = longMask(numBits);
        long valueHighPart = (shiftRight < 0) ? (bitsUpper << -shiftRight) & mask : (bitsUpper >>> shiftRight) & mask;
        long valueLowPart = (shiftRight < 0) ? (bitsLower >>> (64 + shiftRight)) & longMask(-shiftRight) : 0;

        return valueHighPart | valueLowPart;
    }

    public synchronized boolean reset()
    {
        final boolean resetAcquired = resetSemaphore.tryAcquire(ALL_THE_LEASES);
        if (resetAcquired)
        {
            try
            {
                buffer.setMemory(0, 4096, (byte) 0);
                tMinusOne = 0;
                tMinusTwo = 0;
                lastValue = 0.0;
                lastXorValue = 0;

                setLengthInBits(HEADER_LENGTH);
            }
            finally
            {
                resetSemaphore.release(ALL_THE_LEASES);
            }
        }

        return resetAcquired;
    }

    public int compareTo(Block other)
    {
        return buffer.compareTo(other.buffer);
    }

    static long compressBits(long value, int numBits)
    {
        return value & ((1 << (numBits - 1)) - 1) | ((value >>> 63) << (numBits - 1));
    }

    private static long longMask(int numBits)
    {
        assert numBits <= 64;
        return numBits == 64 ? 0xFFFFFFFF_FFFFFFFFL : (1L << numBits) - 1;
    }

    private static int intMask(int numBits)
    {
        assert numBits <= 32;
        return numBits == 32 ? 0xFFFFFFFF : (1 << numBits) - 1;
    }

    static boolean valueInRange(int bitLength, long value)
    {
        return (value & ~longMask(bitLength)) == 0;
    }

    static long decompressBits(long value, int numBits)
    {
        long sign = value >>> numBits - 1;
        long mask = (1 << (numBits - 1)) - 1;
        return value & mask | -sign & ~mask;
    }

    public void copyTo(Block block)
    {
        if (resetSemaphore.tryAcquire())
        {
            try
            {
                final int bitLength = lengthInBits();
                buffer.getBytes(0, block.buffer, 0, 4096);

                block.setLengthInBits(bitLength);
                block.zeroRemaining();
            }
            finally
            {
                resetSemaphore.release();
            }
        }
    }

    private void zeroRemaining()
    {
        final int intAlignedByteIndex = (lengthInBits() / 32) * 4;
        final int bitOffset = lengthInBits() % 32;
        final int remainingValue = buffer.getInt(intAlignedByteIndex, BIG_ENDIAN) & (intMask(bitOffset) << (32 - bitOffset));
        buffer.putInt(intAlignedByteIndex, remainingValue, BIG_ENDIAN);

        for (int i = intAlignedByteIndex + 4; i < INT_LENGTH; i += 4)
        {
            buffer.putInt(i, 0);
        }
    }

    @Override
    public String toString()
    {
        return "Block{" +
            "bitLength=" + lengthInBits() +
            ", tMinusOne=" + tMinusOne +
            ", tMinusTwo=" + tMinusTwo +
            ", lastValue=" + lastValue +
            ", lastXorValue=" + lastXorValue +
            '}';
    }
}
