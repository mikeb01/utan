package com.lmax.utan.store;

public class Entry
{
    public final long timestamp;
    public final double value;

    public Entry(long timestamp, double value)
    {
        this.timestamp = timestamp;
        this.value = value;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Entry entry = (Entry) o;

        return timestamp == entry.timestamp && Double.compare(entry.value, value) == 0;
    }

    @Override
    public int hashCode()
    {
        int result;
        long temp;
        result = (int) (timestamp ^ (timestamp >>> 32));
        temp = Double.doubleToLongBits(value);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
}
