package com.lmax.io;

import org.agrona.IoUtil;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.function.Predicate;

public class Dirs
{
    public static File createTempDir(String prefix)
    {
        return createTempDir(prefix, System.getProperty("java.io.tmpdir"));
    }

    private static File createTempDir(String prefix, String parent)
    {
        File f = new File(parent, prefix + "-" + UUID.randomUUID().toString());
        if (!f.mkdirs())
        {
            throw new RuntimeException();
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> IoUtil.delete(f, true)));

        return f;
    }

    public static void ensureDirExists(File dir) throws IOException
    {
        if (!dir.mkdirs() && (!dir.exists() || !dir.isDirectory()))
        {
            throw new IOException("Directory " + dir + " is not valid");
        }
    }

    public static File nextSibling(File f, Predicate<String> namePredicate)
    {
        File parent = f.getParentFile();

        File[] laterSiblings = parent.listFiles(pathname -> namePredicate.test(pathname.getName()) && f.compareTo(pathname) < 0);
        if (null == laterSiblings)
        {
            return null;
        }

        return min(laterSiblings);
    }

    public static File firstInDir(File parent, Predicate<String> namePredicate)
    {
        File[] laterSiblings = parent.listFiles(pathname -> namePredicate.test(pathname.getName()));
        if (null == laterSiblings)
        {
            return null;
        }

        return min(laterSiblings);
    }

    private static <T extends Comparable<T>> T min(T[] values)
    {
        T result = null;

        for (T value : values)
        {
            result = null == result ? value : min(result, value);
        }

        return result;
    }

    private static <T extends Comparable<T>> T min(T a, T b)
    {
        if (a.compareTo(b) <= 0)
        {
            return a;
        }
        return b;
    }
}
