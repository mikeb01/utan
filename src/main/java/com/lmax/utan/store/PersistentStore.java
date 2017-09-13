package com.lmax.utan.store;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.OpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Set;
import java.util.regex.Pattern;

import static com.lmax.utan.io.Dirs.ensureDirExists;
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

    static File getKeyDir(File parent, CharSequence key, boolean createIfNotExists) throws IOException
    {
        return getKeyDir(parent, key.toString().getBytes(StandardCharsets.UTF_8), createIfNotExists);
    }

    static File getKeyDir(File parent, byte[] keyAsBytes, boolean createIfNotExists) throws IOException
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
        File timePath = keyDir.toPath().resolve(formatAsDate(timestamp)).toFile();
        if (createIfNotExists)
        {
            ensureDirExists(timePath);
        }

        return timePath;
    }

    private static String formatAsDate(long timestamp)
    {
        final LocalDate date = Instant.ofEpochMilli(timestamp).atOffset(ZoneOffset.UTC).toLocalDate();
        final int year = date.getYear();
        final int monthOfYear = date.getMonthValue();
        final int dayOfMonth = date.getDayOfMonth();

        return String.valueOf(year) + '-' +
        (monthOfYear < 10 ? "0" : "") + monthOfYear + '-' +
        (dayOfMonth < 10 ? "0" : "") + dayOfMonth;
    }

    private static final Pattern DATE_PATTERN = Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}");
    static boolean isTimeDir(String s)
    {
        return DATE_PATTERN.matcher(s).matches();
    }
}
