package com.lmax.utan.store;

import com.lmax.collection.Strings;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Set;

import static com.lmax.io.Dirs.ensureDirExists;
import static java.lang.ThreadLocal.withInitial;
import static org.agrona.BitUtil.toHex;

public class PersistentStore
{
    static final ThreadLocal<MessageDigest> MESSAGE_DIGEST_LOCAL = withInitial(
        () ->
        {
            try
            {
                return MessageDigest.getInstance("SHA-1");
            }
            catch (NoSuchAlgorithmException e)
            {
                throw new RuntimeException(e);
            }
        });

    static File getKeyDir(byte[] keyAsBytes, boolean createIfNotExists, File parent) throws IOException
    {
        final MessageDigest sha1Digest = MESSAGE_DIGEST_LOCAL.get();

        sha1Digest.reset();
        byte[] digest = sha1Digest.digest(keyAsBytes);

        String digestString = toHex(digest);
        String prefix = digestString.substring(0, 3);
        String suffix = digestString.substring(3);

        File keyPath = parent.toPath().resolve(prefix).resolve(suffix).toFile();

        if (createIfNotExists)
        {
            ensureDirExists(keyPath);
        }

        return keyPath;
    }

    static FileChannel getTimeSeriesChannel(File timeDir, Set<? extends OpenOption> openOptions) throws IOException
    {
        return FileChannel.open(timeDir.toPath().resolve("timeseries.dat"), openOptions);
    }

    static File getTimeDir(File keyDir, long timestamp, boolean createIfNotExists) throws IOException
    {
        final LocalDate date = Instant.ofEpochMilli(timestamp).atOffset(ZoneOffset.UTC).toLocalDate();
        return getTimeDir(keyDir, date, createIfNotExists);
    }

    private static File getTimeDir(File keyDir, LocalDate dateTime, boolean createIfNotExists) throws IOException
    {
        String year = String.valueOf(dateTime.getYear());
        String month = Strings.lPad2(dateTime.getMonth().getValue());
        String day = Strings.lPad2(dateTime.getDayOfMonth());

        File timePath = keyDir.toPath().resolve(year).resolve(month).resolve(day).toFile();
        if (createIfNotExists)
        {
            ensureDirExists(timePath);
        }

        return timePath;
    }
}
