package com.lmax.utan.store;

import com.lmax.collection.Strings;
import com.lmax.io.Dirs;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.IntStream;

import static java.nio.file.StandardOpenOption.READ;
import static java.util.stream.Collectors.toSet;

public class PersistentStoreReader
{
    private static final Set<String> VALID_DAYS = IntStream.range(1, 32).mapToObj(Strings::lPad2).collect(toSet());
    private static final Set<String> VALID_MONTHS = IntStream.range(1, 12).mapToObj(Strings::lPad2).collect(toSet());
    private static final Set<String> VALID_YEARS = IntStream.range(2012, 2112).mapToObj(Strings::lPad2).collect(toSet());

    private static final Set<? extends OpenOption> READ_ONLY_OPTIONS = EnumSet.of(READ);

    private final File dir;

    public PersistentStoreReader(File dir)
    {
        this.dir = dir;
    }

    public Block findBlockContainingTimestamp(CharSequence key, long timestamp) throws IOException
    {
        byte[] keyAsBytes = key.toString().getBytes(StandardCharsets.UTF_8);
        File keyDir = PersistentStore.getKeyDir(keyAsBytes, false, dir);

        if (!keyDir.exists())
        {
            throw new NoSuchFileException("Key directory: " + keyDir.toString());
        }

        File timeDir = PersistentStore.getTimeDir(keyDir, timestamp, false);

        if (!timeDir.exists())
        {
            timeDir = nextDir(timeDir);
        }

        try (FileChannel timeSeries = PersistentStore.getTimeSeriesChannel(timeDir, READ_ONLY_OPTIONS))
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
                try (FileChannel nextTimeSeries = PersistentStore.getTimeSeriesChannel(nextTimeDir, READ_ONLY_OPTIONS))
                {
                    if (nextTimeSeries.size() > 0)
                    {
                        return readBlock(nextTimeSeries, block, 0);
                    }
                }
            }
        }

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

}
