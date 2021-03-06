package com.lmax.utan.store;

import java.util.List;
import java.util.function.Supplier;

public class BlockGenerator
{
    public static void generateBlockData(
        final Supplier<Entry> supplier,
        final Block block)
    {
        generateBlockData(supplier, block, null);
    }

    public static List<Entry> generateBlockData(
        final Supplier<Entry> supplier,
        final Block block,
        final List<Entry> entries)
    {
        do
        {
            Entry entry = supplier.get();
            if (!block.append(entry.timestamp, entry.value).isOk())
            {
                break;
            }

            if (null != entries)
            {
                entries.add(entry);
            }
        }
        while (true);

        return entries;
    }
}
