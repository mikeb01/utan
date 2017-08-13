package com.lmax.collection;

import java.util.Set;

public class Sets
{
    public static <T> Set<T> setOf(Set<T> objects, T... ts)
    {
        for (T t : ts)
        {
            objects.add(t);
        }

        return objects;
    }
}
