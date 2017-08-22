package com.lmax.utan.collection;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class Maps
{
    public static <K, V, T> Map<K, V> mapOf(Iterable<T> source, BiConsumer<T, Map<K, V>> transform, Supplier<Map<K, V>> mapSupplier)
    {
        Map<K, V> map = mapSupplier.get();
        for (T t : source)
        {
            transform.accept(t, map);
        }

        return map;
    }
}
