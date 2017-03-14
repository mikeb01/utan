package com.lmax.utan.store;

class Entry
{
    final long timestamp;
    final double value;

    Entry(long timestamp, double value)
    {
        this.timestamp = timestamp;
        this.value = value;
    }
}
