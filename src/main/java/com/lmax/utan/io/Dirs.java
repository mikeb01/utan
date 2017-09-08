package com.lmax.utan.io;

import org.agrona.IoUtil;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.function.Predicate;


import static java.lang.Math.max;

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
        if (!dir.exists())
        {
            if (!dir.mkdirs())
            {
                throw new IOException("Directory " + dir + " is not valid");
            }
            else if (!dir.exists())
            {
                throw new IOException("Created: " + dir + ", but it still does not exist");
            }
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

    public static File lastInDir(File parent, Predicate<String> namePredicate)
    {
        File[] laterSiblings = parent.listFiles(pathname -> namePredicate.test(pathname.getName()));
        if (null == laterSiblings)
        {
            return null;
        }

        return max(laterSiblings);
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

    private static <T extends Comparable<T>> T max(T[] values)
    {
        T result = null;

        for (T value : values)
        {
            result = null == result ? value : max(result, value);
        }

        return result;
    }

    private static <T extends Comparable<T>> T max(T a, T b)
    {
        if (a.compareTo(b) >= 0)
        {
            return a;
        }
        return b;
    }

    public static void delete(final File file) throws IOException
    {
        if (file == null)
        {
            return;
        }

        if (file.isDirectory())
        {
            deleteDir(file);
        }
        else
        {
            deleteFile(file);
        }
    }

    public static void deleteDir(final File dir) throws IOException
    {
        final File[] childFiles = dir.listFiles(pathname -> !pathname.isDirectory());
        if (childFiles == null)
        {
            return; // Directory doesn't exist, nothing more to do.
        }
        for (final File child : childFiles)
        {
            deleteFile(child);
        }
        final File[] childDirs = dir.listFiles(File::isDirectory);
        if (childDirs == null)
        {
            return; // Directory got deleted by someone else, nothing more to do.
        }
        for (final File childDir : childDirs)
        {
            deleteDir(childDir);
        }

        deleteFile(dir);
    }

    private static void deleteFile(File child) throws IOException
    {
        if (child.exists() && !child.delete())
        {
            throw new IOException("Failed to delete: " + child);
        }
    }
}
