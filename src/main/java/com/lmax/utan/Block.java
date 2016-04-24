package com.lmax.utan;

import org.agrona.BitUtil;
import org.agrona.concurrent.AtomicBuffer;

public class Block
{
    public static final int INITIAL_LENGTH = 32;
    public static final int FIRST_TIMESTAMP_OFFSET = 4;
    public static final int FIRST_VALUE_OFFSET = 12;

    private final AtomicBuffer buffer;
    private long tMinusOne = 0;
    private long tMinusTwo = 0;
    private double lastValue = 0.0;


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
        if (lenghtInBits() == INITIAL_LENGTH)
        {
            appendInitial(timestamp, val);
        }
        else
        {
            appendCompressed(timestamp, val);
        }
    }

    private void appendInitial(long timestamp, double val)
    {
        buffer.putLong(FIRST_TIMESTAMP_OFFSET, timestamp);
        buffer.putDouble(FIRST_VALUE_OFFSET, val);
        setLength(160);

        tMinusOne = timestamp;
        tMinusTwo = timestamp;
        lastValue = val;
    }

    private void appendCompressed(long timestamp, double val)
    {
        int length = lenghtInBits();

        long d = (timestamp - tMinusOne) - (tMinusOne - tMinusTwo);

        final int timestampBitsAdded;
        if (d == 0)
        {
            timestampBitsAdded = appendZeroTimestampDelta(length);
        }
        else if (-63 <= d && d <= 64)
        {
            timestampBitsAdded = appendTimestampDelta(0b10, d);
        }
        else if (-255 <= d && d <= 256)
        {
            timestampBitsAdded = appendTimestampDelta(0b110, d);
        }
        else if (-2047 <= d && d <= 2048)
        {
            timestampBitsAdded = appendTimestampDelta(0b1110, d);
        }
        else
        {
            timestampBitsAdded = appendTimestampDelta(0b11110, d);
        }

        long valueAsLong = Double.doubleToLongBits(val);
        long lastValueAsLong = Double.doubleToLongBits(val);
        long xorValue = valueAsLong ^ lastValueAsLong;

        final int valueBitsAdded;
        if (xorValue == 0)
        {
            valueBitsAdded = appendZeroValueXor();
        }
        else
        {
            valueBitsAdded = appendValueXor(xorValue);
        }

        int newLength = length + timestampBitsAdded + valueBitsAdded;

        setLength(newLength);
    }

    private int appendZeroTimestampDelta(int bitPosition)
    {
        return 1;
    }

    private int appendTimestampDelta(int markerBits, long valueBits)
    {
        return 0;
    }

    private int appendZeroValueXor()
    {
        return 1;
    }

    private int appendValueXor(long xorValue)
    {
        return 0;
    }

    public int lenghtInBits()
    {
        return buffer.getIntVolatile(0);
    }

    public int foreach(ValueConsumer consumer)
    {
        int lengthInBits = lenghtInBits();

        if (lengthInBits <= INITIAL_LENGTH)
        {
            return 0;
        }

        long timestamp = buffer.getLong(FIRST_TIMESTAMP_OFFSET);
        double value = buffer.getDouble(FIRST_VALUE_OFFSET);

        consumer.accept(timestamp, value);

        long tMinusOne = timestamp;
        long tMinusTwo = timestamp;

        int bitOffset = INITIAL_LENGTH + ((BitUtil.SIZE_OF_LONG + BitUtil.SIZE_OF_LONG) * 8);

        int count = 1;
        while (bitOffset < lengthInBits)
        {
            if (0 == getNextBits(bitOffset, 1))
            {
                timestamp =  0 + (tMinusTwo - tMinusOne) + tMinusOne;
                bitOffset += 1;
            }
            else
            {
                throw new RuntimeException("TDD");
            }


            if (0 == getNextBits(bitOffset, 1))
            {
                bitOffset += 1;
            }
            else
            {
                throw new RuntimeException("TDD");
            }

            consumer.accept(timestamp, value);
            count++;
        }

        return count;
    }

    private int getNextBits(int offset, int numBits)
    {
        int byteOffset = offset / 8;
        int bitSubIndex = offset & 8;

        long bits = buffer.getLong(byteOffset);
        long mask = (1 << numBits) - 1;
        long shift = (64 - bitSubIndex) - numBits;
        long shiftedMask = mask << shift;
        long bitsOfInterest = (bits & shiftedMask) >>> shift;

        return (int) (bitsOfInterest & 0xFFFFFFFFL);
    }

    public interface ValueConsumer
    {
        void accept(long timestamp, double value);
    }
}
