package com.lmax.utan.store;

import org.agrona.BitUtil;
import org.agrona.concurrent.AtomicBuffer;

import static java.lang.Long.highestOneBit;
import static java.lang.Long.numberOfTrailingZeros;
import static java.lang.String.format;
import static java.nio.ByteOrder.BIG_ENDIAN;

public class Block
{
    private static final int INITIAL_LENGTH = 64;
    private static final int COMPRESSED_DATA_START = INITIAL_LENGTH + 128;
    private static final int FIRST_TIMESTAMP_OFFSET = 8;
    private static final int FIRST_VALUE_OFFSET = 16;

    private final AtomicBuffer buffer;
    private long tMinusOne = 0;
    private long tMinusTwo = 0;
    private double lastValue = 0.0;
    private long lastXorValue = 0;

    public Block(AtomicBuffer buffer)
    {
        this.buffer = buffer;
        setLength(INITIAL_LENGTH);
    }

    private void setLength(int length)
    {
        buffer.putIntOrdered(0, length);
    }

    public void append(long timestamp, double val)
    {
        int bitOffset = lengthInBits();
        if (bitOffset == INITIAL_LENGTH)
        {
            appendInitial(timestamp, val);
        }
        else
        {
            appendCompressed(bitOffset, timestamp, val);
        }
    }

    private void appendInitial(long timestamp, double val)
    {
        buffer.putLong(FIRST_TIMESTAMP_OFFSET, timestamp, BIG_ENDIAN);
        buffer.putDouble(FIRST_VALUE_OFFSET, val, BIG_ENDIAN);
        setLength(COMPRESSED_DATA_START);

        tMinusOne = timestamp;
        tMinusTwo = timestamp;
        lastValue = val;
    }

    private void appendCompressed(int bitOffset, long timestamp, double val)
    {
        long d = (timestamp - tMinusOne) - (tMinusOne - tMinusTwo);

        final int timestampBitsAdded;
        if (d == 0)
        {
            timestampBitsAdded = appendZeroTimestampDelta();
        }
        else if (-64 <= d && d <= 63)
        {
            timestampBitsAdded = appendTimestampDelta(bitOffset, 7, 0b10, d);
        }
        else if (-256 <= d && d <= 255)
        {
            timestampBitsAdded = appendTimestampDelta(bitOffset, 9, 0b110, d);
        }
        else if (-2048 <= d && d <= 2047)
        {
            timestampBitsAdded = appendTimestampDelta(bitOffset, 12, 0b1110, d);
        }
        else if (Integer.MIN_VALUE <= d && d <= Integer.MAX_VALUE)
        {
            timestampBitsAdded = appendTimestampDelta(bitOffset, 32, 0b11110, d);
        }
        else
        {
            throw new IllegalArgumentException("Timestamp delta out of range: " + d);
        }

        long valueAsLong = Double.doubleToLongBits(val);
        long lastValueAsLong = Double.doubleToLongBits(lastValue);
        long xorValue = valueAsLong ^ lastValueAsLong;

        final int valueBitsAdded;
        if (xorValue == 0)
        {
            valueBitsAdded = appendZeroValueXor(bitOffset + timestampBitsAdded);
        }
        else
        {
            valueBitsAdded = appendValueXor(bitOffset + timestampBitsAdded, xorValue, lastXorValue);
        }

        int newLength = bitOffset + timestampBitsAdded + valueBitsAdded;

        tMinusTwo = tMinusOne;
        tMinusOne = timestamp;
        lastValue = val;
        lastXorValue = xorValue;

        setLength(newLength);
    }

    private int appendZeroTimestampDelta()
    {
        return 1;
    }

    private int appendTimestampDelta(int bitOffset, int numBits, long markerBits, long timestampDelta)
    {
        int markerBitLength = numberOfTrailingZeros(highestOneBit(markerBits) << 1);

        writeBits(bitOffset, markerBitLength, markerBits);
        writeBits(bitOffset + markerBitLength, numBits, compressBits(timestampDelta, numBits));

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
            length += writeBits(bitOffset, 2, 0b10);
            length += writeBits(bitOffset + 2, 64 - (prevLeadingZeros + prevTrailingZeros), xorValue >>> prevTrailingZeros);
        }
        else
        {
            int relevantLength = 64 - (leadingZeros + trailingZeros);

            length += writeBits(bitOffset,                          2, 0b11);
            length += writeBits(bitOffset + 2,                      5, leadingZeros);
            length += writeBits(bitOffset + 2 + 5,                  6, relevantLength - 1);
            length += writeBits(bitOffset + 2 + 5 + 6, relevantLength, xorValue >>> trailingZeros);
        }

        return length;
    }

    public int lengthInBits()
    {
        return buffer.getIntVolatile(0);
    }

    public int foreach(ValueConsumer consumer)
    {
        int lengthInBits = lengthInBits();

        if (lengthInBits <= INITIAL_LENGTH)
        {
            return 0;
        }

        long timestamp = buffer.getLong(FIRST_TIMESTAMP_OFFSET, BIG_ENDIAN);
        double value = buffer.getDouble(FIRST_VALUE_OFFSET, BIG_ENDIAN);
        long lastXorValue = 0;

        consumer.accept(timestamp, value);

        long tMinusOne = timestamp;
        long tMinusTwo = timestamp;

        int bitOffset = INITIAL_LENGTH + ((BitUtil.SIZE_OF_LONG + BitUtil.SIZE_OF_LONG) * 8);

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
        long bitsLower = buffer.getLong(longAlignedByteOffset + 8, BIG_ENDIAN);

        int shiftRight = (64 - bitSubIndex) - numBits;

        long mask = mask(numBits);
        long valueHighPart = (shiftRight < 0) ? (bitsUpper << -shiftRight) & mask : (bitsUpper >>> shiftRight) & mask;
        long valueLowPart = (shiftRight < 0) ? (bitsLower >>> (64 + shiftRight)) & mask(-shiftRight) : 0;

        return valueHighPart | valueLowPart;
    }

    private int writeBits(int bitOffset, int bitLength, long value)
    {
        assert valueInRange(bitLength, value) : format("writeBits(%d, %d (%d), %d)", bitOffset, bitLength, 1L << bitLength, value);

        int longAlignedByteOffset = (bitOffset / 64) * 8;
        int bitSubIndex = bitOffset & 63;
        int shiftLeft = (64 - bitSubIndex) - bitLength;

        long valueHighPart = (shiftLeft < 0) ? value >>> -shiftLeft : value << shiftLeft;
        long valueLowPart = (shiftLeft < 0) ? value << 64 + shiftLeft : 0;

        long bits = buffer.getLong(longAlignedByteOffset, BIG_ENDIAN);
        buffer.putLong(longAlignedByteOffset, bits | valueHighPart, BIG_ENDIAN);
        buffer.putLong(longAlignedByteOffset + 8, valueLowPart, BIG_ENDIAN);

        return bitLength;
    }

    public interface ValueConsumer
    {
        void accept(long timestamp, double value);
    }

    static long compressBits(long value, int numBits)
    {
        return value & ((1 << (numBits - 1)) - 1) | ((value >>> 63) << (numBits - 1));
    }

    private static long mask(int numBits)
    {
        assert numBits <= 64;
        return numBits == 64 ? 0xFFFFFFFF_FFFFFFFFL : (1L << numBits) - 1;
    }

    static boolean valueInRange(int bitLength, long value)
    {
        return (value & ~mask(bitLength)) == 0;
    }

    static long decompressBits(long value, int numBits)
    {
        long sign = value >>> numBits - 1;
        long mask = (1 << (numBits - 1)) - 1;
        return value & mask | -sign & ~mask;
    }
}
