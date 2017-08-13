package com.lmax.utan.store;

import com.lmax.io.Dirs;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

import static com.lmax.collection.Sets.setOf;
import static com.lmax.io.Dirs.ensureDirExists;
import static java.lang.ThreadLocal.withInitial;
import static java.nio.file.StandardOpenOption.READ;
import static java.util.stream.Collectors.toSet;

public class PersistentStoreReader
{
    private static final Set<String> VALID_DAYS = IntStream.range(1, 32).mapToObj(PersistentStoreReader::lPad2).collect(toSet());
    private static final Set<String> VALID_MONTHS = IntStream.range(1, 12).mapToObj(PersistentStoreReader::lPad2).collect(toSet());
    private static final Set<String> VALID_YEARS = IntStream.range(2012, 2013).mapToObj(PersistentStoreReader::lPad2).collect(toSet());
    private static final Set<? extends OpenOption> OPEN_OPTIONS = setOf(new HashSet<>(), READ);

    private final File dir;

    private final ThreadLocal<MessageDigest> messageDigestLocal = withInitial(
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

    public PersistentStoreReader(File dir)
    {
        this.dir = dir;
    }

    public Block findBlockContainingTimestamp(CharSequence key, long timestamp) throws IOException
    {
        byte[] keyAsBytes = key.toString().getBytes(StandardCharsets.UTF_8);
        File keyDir = PersistentStore.getKeyDir(keyAsBytes, false, messageDigestLocal.get(), dir);

        if (!keyDir.exists())
        {
            throw new NoSuchFileException("Key directory: " + keyDir.toString());
        }

        final LocalDate date = Instant.ofEpochMilli(timestamp).atOffset(ZoneOffset.UTC).toLocalDate();
        File timeDir = getTimeDir(keyDir, date, false);

        if (!timeDir.exists())
        {
            timeDir = nextDir(timeDir);
        }

        try (FileChannel timeSeries = getChannel(timeDir))
        {
            if (timeSeries.size() < Block.BYTE_LENGTH)
            {
                throw new IOException("No data in time series: " + timeSeries);
            }

            Block block = Block.new4kDirectBlock();
            long previousPosition = 0;
            long currentPosition = 0;

            do
            {
                block.underlyingBuffer().clear();
                timeSeries.read(block.underlyingBuffer(), currentPosition);
                if (timestamp < block.firstTimestamp())
                {
                    block.underlyingBuffer().clear();
                    timeSeries.read(block.underlyingBuffer(), previousPosition);
                    return block;
                }
                previousPosition = currentPosition;
                currentPosition += Block.BYTE_LENGTH;
            }
            while (currentPosition < timeSeries.size());

            long lastBlockPosition = timeSeries.size() - Block.BYTE_LENGTH;
            block = readBlock(timeSeries, block, lastBlockPosition);
            if (timestamp <= block.lastTimestamp())
            {
                return block;
            }

            File nextTimeDir = nextDir(timeDir);

            if (nextTimeDir != null)
            {
                try (FileChannel nextTimeSeries = getChannel(nextTimeDir))
                {
                    if (nextTimeSeries.size() > 0)
                    {
                        return readBlock(nextTimeSeries, block, 0);
                    }
                }
            }
        }

        // TODO: Find next timeseries.
        throw new IOException("Request out of range");
    }

    private static File nextDir(File timeDir)
    {
        File nextDay = Dirs.nextSibling(timeDir, VALID_DAYS::contains);

        if (null != nextDay)
        {
            return nextDay;
        }

        final File monthDir = timeDir.getParentFile();
        File nextMonth = Dirs.nextSibling(monthDir, VALID_MONTHS::contains);

        if (null != nextMonth)
        {
            nextDay = Dirs.firstInDir(nextMonth, VALID_DAYS::contains);
            if (null != nextDay)
            {
                return nextDay;
            }
        }

        final File yearDir = monthDir.getParentFile();
        final File nextYear = Dirs.nextSibling(yearDir, VALID_YEARS::contains);

        if (null != nextYear)
        {
            nextMonth = Dirs.firstInDir(yearDir, VALID_MONTHS::contains);
            if (null != nextMonth)
            {
                nextDay = Dirs.firstInDir(nextMonth, VALID_DAYS::contains);
                if (null != nextDay)
                {
                    return nextDay;
                }
            }
        }

        return null;
    }

    private Block readBlock(FileChannel fileChannel, Block block, long position) throws IOException
    {
        block.underlyingBuffer().clear();
        fileChannel.read(block.underlyingBuffer(), position);
        return block;
    }

    private FileChannel getChannel(File timeDir) throws IOException
    {
        File timeSeriesFile = new File(timeDir, "timeseries.dat");
        return FileChannel.open(timeSeriesFile.toPath(), OPEN_OPTIONS);
    }

    private File getTimeDir(File keyDir, LocalDate offsetDateTime, boolean createIfNotExists) throws IOException
    {
        String year = String.valueOf(offsetDateTime.getYear());
        String month = lPad2(offsetDateTime.getMonth().getValue());
        String day = lPad2(offsetDateTime.getDayOfMonth());

        File timePath = keyDir.toPath().resolve(year).resolve(month).resolve(day).toFile();
        if (createIfNotExists)
        {
            ensureDirExists(timePath);
        }

        return timePath;
    }

    private static String lPad2(int value)
    {
        String pad = value < 10 ? "0" : "";
        return pad + value;
    }
}
