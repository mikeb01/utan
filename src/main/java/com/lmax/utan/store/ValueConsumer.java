package com.lmax.utan.store;

public interface ValueConsumer
{
    boolean accept(long timestamp, double value);
}
