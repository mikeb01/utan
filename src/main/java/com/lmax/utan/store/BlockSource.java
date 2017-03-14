package com.lmax.utan.store;

public interface BlockSource
{
    Iterable<Block> lastN(int n);
}
