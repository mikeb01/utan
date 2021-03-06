package com.lmax.utan.store;

import org.agrona.BitUtil;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.concurrent.Semaphore;

import static java.lang.Long.highestOneBit;
import static java.lang.Long.numberOfTrailingZeros;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.nio.ByteOrder.BIG_ENDIAN;

public class Block implements Comparable<Block>
{
    public static final int BYTE_LENGTH = 512;
    public static final ByteOrder BYTE_ORDER = BIG_ENDIAN;

    private static final int HEADER_LENGTH_BITS = 64;
    static final int COMPRESSED_DATA_START_BITS = HEADER_LENGTH_BITS + 128;
    public static final int FIRST_TIMESTAMP_OFFSET = HEADER_LENGTH_BITS / 8;
    private static final int FIRST_VALUE_OFFSET = FIRST_TIMESTAMP_OFFSET + 8;
    private static final int BIT_LENGTH_LIMIT = BYTE_LENGTH * 8;
    private static final int INT_LENGTH = BYTE_LENGTH / 4;
    private static final int ALL_THE_LEASES = 1024;

    private static final int TS_SHORT_MIN = -64;
    private static final int TS_SHORT_MAX = 63;
    private static final int TS_SHORT_PREFIX = 0b10;
    private static final int TS_MED_MIN = -256;
    private static final int TS_MED_MAX = 255;
    private static final int TS_MED_PREFIX = 0b110;
    private static final int TS_LONG_MIN = -2048;
    private static final int TS_LONG_MAX = 2047;
    private static final int TS_LONG_PREFIX = 0b1110;
    private static final int TS_FULL_PREFIX = 0b11110;
    public static final int TS_SHORT_NBITS = 7;
    public static final int TS_MED_NBITS = 9;
    public static final int TS_LONG_NBITS = 12;
    public static final int TS_FILL_NBITS = 32;

    private final BlockHeader header;
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

    public ByteBuffer underlyingBuffer()
    {
        ByteBuffer byteBuffer = buffer.byteBuffer();
        return byteBuffer == null ? ByteBuffer.wrap(buffer.byteArray()) : byteBuffer;
    }

    public enum AppendStatus
    {
        OK,
        FULL,
        FROZEN;

        public boolean isOk()
        {
            return this == OK;
        }

    }
    public Block()
    {
        this(new UnsafeBuffer(new byte[BYTE_LENGTH]));
    }

    public Block(AtomicBuffer buffer)
    {
        this.buffer = buffer;
        this.header = new BlockHeader(buffer);
        reset();
    }

    public static Block newHeapBlock()
    {
        return new Block(new UnsafeBuffer(new byte[BYTE_LENGTH]));
    }

    public static Block newDirectBlock()
    {
        return new Block(new UnsafeBuffer(ByteBuffer.allocateDirect(BYTE_LENGTH)));
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

    public int lengthInBits()
    {
        return header.lengthInBits();
    }

    public boolean isFrozen()
    {
        return header.isFrozen();
    }

    public long firstTimestamp()
    {
        return header.firstTimestamp();
    }

    public long lastTimestamp()
    {
        return header.lastTimestamp();
    }

    // ====================
    // Writing to the block
    // ====================

    public synchronized AppendStatus append(long timestamp, double val)
    {
        if (isFrozen())
        {
            return AppendStatus.FROZEN;
        }

        int bitOffset = header.lengthInBits();

        if (bitOffset == HEADER_LENGTH_BITS)
        {
            appendInitial(timestamp, val);
            return AppendStatus.OK;
        }
        else
        {
            return appendCompressed(bitOffset, timestamp, val);
        }
    }

    public boolean isEmpty()
    {
        return header.lengthInBits() == HEADER_LENGTH_BITS;
    }

    private void appendInitial(long timestamp, double val)
    {
        buffer.putLong(FIRST_TIMESTAMP_OFFSET, timestamp, BYTE_ORDER);
        buffer.putDouble(FIRST_VALUE_OFFSET, val, BYTE_ORDER);
        header.writeHeader(false, COMPRESSED_DATA_START_BITS, 0);

        tMinusOne = timestamp;
        tMinusTwo = timestamp;
        lastValue = val;
    }

    private AppendStatus appendCompressed(int bufferBitIndex, long timestamp, double val)
    {
        resetBitBuffer();

        final long d = (timestamp - tMinusOne) - (tMinusOne - tMinusTwo);

        final int timestampBitsAdded;
        if (d == 0)
        {
            timestampBitsAdded = appendZeroTimestampDelta();
        }
        else if (TS_SHORT_MIN <= d && d <= TS_SHORT_MAX)
        {
            timestampBitsAdded = appendTimestampDelta(0, TS_SHORT_NBITS, TS_SHORT_PREFIX, (int) d - TS_SHORT_MIN);
        }
        else if (TS_MED_MIN <= d && d <= TS_MED_MAX)
        {
            timestampBitsAdded = appendTimestampDelta(0, TS_MED_NBITS, TS_MED_PREFIX, (int) d - TS_MED_MIN);
        }
        else if (TS_LONG_MIN <= d && d <= TS_LONG_MAX)
        {
            timestampBitsAdded = appendTimestampDelta(0, TS_LONG_NBITS, TS_LONG_PREFIX, (int) d - TS_LONG_MIN);
        }
        else if (Integer.MIN_VALUE <= d && d <= Integer.MAX_VALUE)
        {
            timestampBitsAdded = appendTimestampDelta(0, TS_FILL_NBITS, TS_FULL_PREFIX, (int) d);
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
            return AppendStatus.FULL;
        }

        flushTemp(bufferBitIndex, totalBitsAdded);

        tMinusTwo = tMinusOne;
        tMinusOne = timestamp;
        lastValue = val;
        lastXorValue = xorValue;

        header.writeHeader(false, newBitLength, (int) (timestamp - firstTimestamp()));

        return AppendStatus.OK;
    }

    private int appendZeroTimestampDelta()
    {
        return 1;
    }

    private int appendTimestampDelta(int bitOffset, int numBits, int markerBits, int timestampDelta)
    {
        int markerBitLength = numberOfTrailingZeros(highestOneBit(markerBits) << 1);

        writeBits(bitOffset, markerBits, markerBitLength);
        writeBits(bitOffset + markerBitLength, timestampDelta, numBits);

        return markerBitLength + numBits;
    }

    private int appendZeroValueXor(@SuppressWarnings("unused") int i)
    {
        return 1;
    }

    private int appendValueXor(int bitOffset, long xorValue, long previousXorValue)
    {
        int leadingZeros = min(31, Long.numberOfLeadingZeros(xorValue));
        int trailingZeros = Long.numberOfTrailingZeros(xorValue);
        int prevLeadingZeros = Long.numberOfLeadingZeros(previousXorValue);
        int prevTrailingZeros = Long.numberOfTrailingZeros(previousXorValue);

        int length = 0;

        if (leadingZeros >= prevLeadingZeros && trailingZeros >= prevTrailingZeros)
        {
            length += writeBits(bitOffset, 0b10, 2);
            length += writeBits(
                bitOffset + 2, xorValue >>> prevTrailingZeros, 64 - (prevLeadingZeros + prevTrailingZeros));
        }
        else
        {
            int relevantLength = 64 - (leadingZeros + trailingZeros);

            length += writeBits(bitOffset, 0b11, 2);
            length += writeBits(bitOffset + 2, leadingZeros, 5);
            length += writeBits(bitOffset + 2 + 5, (relevantLength - 1), 6);
            length += writeBits(bitOffset + 2 + 5 + 6, xorValue >>> trailingZeros, relevantLength);
        }

        return length;
    }

    @SuppressWarnings("UnusedReturnValue") // It is actually...
    int writeBits(int tempBitIndex, int value, int valueBitLength)
    {
        return writeBits(tempBitIndex, value & 0xFFFFFFFFL, valueBitLength);
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

        int existingValue = buffer.getInt(intAlignedBufferByteIndex, BYTE_ORDER);

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
                buffer.putInt(intAlignedBufferByteIndex + 16, tempShifted4, BYTE_ORDER);
            case 4:
                buffer.putInt(intAlignedBufferByteIndex + 12, tempShifted3, BYTE_ORDER);
            case 3:
                buffer.putInt(intAlignedBufferByteIndex + 8, tempShifted2, BYTE_ORDER);
            case 2:
                buffer.putInt(intAlignedBufferByteIndex + 4, tempShifted1, BYTE_ORDER);
            case 1:
                buffer.putInt(intAlignedBufferByteIndex, tempShifted0, BYTE_ORDER);
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
        final int lengthInBits = header.lengthInBits();

        if (lengthInBits <= HEADER_LENGTH_BITS)
        {
            return 0;
        }

        long timestamp = buffer.getLong(FIRST_TIMESTAMP_OFFSET, BYTE_ORDER);
        double value = buffer.getDouble(FIRST_VALUE_OFFSET, BYTE_ORDER);
        long lastXorValue = 0;

        if (!consumer.accept(timestamp, value))
        {
            return 1;
        }

        long tMinusOne = timestamp;
        long tMinusTwo = timestamp;

        int bitOffset = HEADER_LENGTH_BITS + ((BitUtil.SIZE_OF_LONG + BitUtil.SIZE_OF_LONG) * 8);

        int count = 1;
        while (bitOffset < lengthInBits)
        {
            final int delta;
            if (0 == readBits(bitOffset, 1))
            {
                bitOffset += 1;
                delta = 0;
            }
            else if (TS_SHORT_PREFIX == readBits(bitOffset, 2))
            {
                bitOffset += 2;
                delta = readBits(bitOffset, TS_SHORT_NBITS) + TS_SHORT_MIN;
                bitOffset += TS_SHORT_NBITS;
            }
            else if (TS_MED_PREFIX == readBits(bitOffset, 3))
            {
                bitOffset += 3;
                delta = readBits(bitOffset, TS_MED_NBITS) + TS_MED_MIN;
                bitOffset += TS_MED_NBITS;
            }
            else if (TS_LONG_PREFIX == readBits(bitOffset, 4))
            {
                bitOffset += 4;
                delta = readBits(bitOffset, TS_LONG_NBITS) + TS_LONG_MIN;
                bitOffset += TS_LONG_NBITS;
            }
            else if (TS_FULL_PREFIX == readBits(bitOffset, 5))
            {
                bitOffset += 5;
                delta = readBits(bitOffset, 32);
                bitOffset += 32;
            }
            else
            {
                throw new IllegalStateException("Data Corrupt");
            }

            timestamp = delta + (tMinusOne - tMinusTwo) + tMinusOne;
            tMinusTwo = tMinusOne;
            tMinusOne = timestamp;

            if (0 == readBits(bitOffset, 1))
            {
                bitOffset += 1;
            }
            else
            {
                final long prefixBits = readBits(bitOffset, 2);
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

            count++;
            if (!consumer.accept(timestamp, value))
            {
                break;
            }
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

        long bitsUpper = buffer.getLong(longAlignedByteOffset, BYTE_ORDER);
        long bitsLower;
        if (longAlignedByteOffset + 8 < buffer.capacity())
        {
            bitsLower = buffer.getLong(longAlignedByteOffset + 8, BYTE_ORDER);
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
                buffer.setMemory(0, BYTE_LENGTH, (byte) 0);
                tMinusOne = 0;
                tMinusTwo = 0;
                lastValue = 0.0;
                lastXorValue = 0;
                resetBitBuffer();

                header.writeHeader(isFrozen(), HEADER_LENGTH_BITS, header.lastTimestampDelta());
            }
            finally
            {
                resetSemaphore.release(ALL_THE_LEASES);
            }
        }

        return resetAcquired;
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

    public void copyTo(Block block)
    {
        if (resetSemaphore.tryAcquire())
        {
            try
            {
                final long headerValue = header.readHeader();
                buffer.getBytes(0, block.buffer, 0, BYTE_LENGTH);

                block.header.writeHeader(headerValue);
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
        final int intAlignedByteIndex = (header.lengthInBits() / 32) * 4;
        final int bitOffset = header.lengthInBits() % 32;
        final int remainingValue = buffer.getInt(intAlignedByteIndex, BYTE_ORDER) & (intMask(bitOffset) << (32 - bitOffset));
        buffer.putInt(intAlignedByteIndex, remainingValue, BYTE_ORDER);

        for (int i = intAlignedByteIndex + 4; i < INT_LENGTH; i += 4)
        {
            buffer.putInt(i, 0);
        }
    }

    @Override
    public String toString()
    {
        return "Block{" +
            "bitLength=" + header.lengthInBits() +
            ", firstTimestamp=" + getUtc(firstTimestamp()) + " (" + firstTimestamp() + ")" +
            ", lastTimestampDelta=" + getUtc(lastTimestamp()) + " (" + lastTimestamp() + ")" +
            ", isFrozen=" + isFrozen() +
            '}';
    }

    static ZonedDateTime getUtc(long firstTimestamp)
    {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(firstTimestamp), ZoneId.of("UTC"));
    }

    public synchronized void freeze()
    {
        header.writeHeader(true, header.lengthInBits(), header.lastTimestampDelta());
    }

    @Override
    public int compareTo(Block other)
    {
        return buffer.compareTo(other.buffer);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Block block = (Block) o;

        for (int i = 0; i < BYTE_LENGTH; i += 8)
        {
            if (buffer.getLong(i) != block.buffer.getLong(i))
            {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return Long.hashCode(firstTimestamp());
    }
}
