package com.lmax.utan.store;

import java.io.IOException;

public interface Cursor<T> extends AutoCloseable
{
    boolean moveNext() throws IOException;

    T current();

    void close();
}
