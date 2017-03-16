package com.lmax.utan.store;

public interface ValueConsumer
{
    void accept(long timestamp, double value);
}
