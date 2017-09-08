package com.lmax.utan.store;

import com.lmax.utan.collection.Strings;
import com.lmax.utan.io.Dirs;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.security.spec.PSSParameterSpec;
import java.util.EnumSet;
import java.util.HashSet;
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
        try (final Cursor<Block> query = query(key, timestamp, Long.MAX_VALUE))
        {
            if (query.moveNext())
            {
                Block block = Block.newHeapBlock();
                final Block current = query.current();
                current.copyTo(block);

                return block;
            }
        }

        return null;
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
            nextMonth = Dirs.firstInDir(nextYear, VALID_MONTHS::contains);
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

    private void readHeader(final BlockHeader blockHeader, final FileChannel timeSeries, final long currentPosition) throws IOException
    {
        blockHeader.underlyingBuffer().clear();
        timeSeries.read(blockHeader.underlyingBuffer(), currentPosition);
    }

    private void readBlock(File timeDir, FileChannel fileChannel, BlockCursor blockCursor, long position) throws IOException
    {
        blockCursor.currentBlock.underlyingBuffer().clear();
        fileChannel.read(blockCursor.currentBlock.underlyingBuffer(), position);
        blockCursor.setLocation(timeDir, fileChannel, position);
    }

    boolean findCurrentBlock(BlockCursor blockCursor) throws IOException
    {
        final BlockHeader blockHeader = BlockHeader.allocateDirect();
        final byte[] keyAsBytes = blockCursor.key.toString().getBytes(StandardCharsets.UTF_8);
        File keyDir = PersistentStore.getKeyDir(dir, keyAsBytes, false);

        if (!keyDir.exists())
        {
            throw new NoSuchFileException("Key directory: " + keyDir.toString());
        }

        File timeDir = PersistentStore.getTimeDir(keyDir, blockCursor.startTimestamp, false);

        if (!timeDir.exists())
        {
            timeDir = nextDir(timeDir);
        }

        if (null == timeDir || !timeDir.exists())
        {
            throw new IOException("No data available for key: " + blockCursor.key + ", timeDir: " + timeDir);
        }

        final FileChannel timeSeries = PersistentStore.getTimeSeriesChannel(timeDir, READ_ONLY_OPTIONS);
        if (timeSeries.size() < Block.BYTE_LENGTH)
        {
            throw new IOException("No data in time series for key: " + blockCursor.key + ", timeDir: " + timeDir);
        }

        long currentPosition = 0;

        // Find the first block in this data file.
        do
        {
            readHeader(blockHeader, timeSeries, currentPosition);
            final long blockLastTimestamp = blockHeader.lastTimestamp();
            if (blockCursor.startTimestamp <= blockLastTimestamp)
            {
                readBlock(timeDir, timeSeries, blockCursor, currentPosition);
                return true;
            }
            currentPosition += Block.BYTE_LENGTH;
        }
        while (currentPosition < timeSeries.size());

        timeSeries.close();

        // Check if the first block in the next day is before the endTimestamp in the query.
        final File nextTimeDir = nextDir(timeDir);
        if (nextTimeDir != null)
        {
            final FileChannel nextTimeSeries = PersistentStore.getTimeSeriesChannel(nextTimeDir, READ_ONLY_OPTIONS);
            if (nextTimeSeries.size() > 0)
            {
                readBlock(nextTimeDir, nextTimeSeries, blockCursor, 0);
                return blockCursor.currentBlock.firstTimestamp() < blockCursor.endTimestamp;
            }
        }

        return false;
    }

    boolean nextBlock(BlockCursor blockCursor) throws IOException
    {
        final boolean nextBlock;

        if (blockCursor.filePosition + Block.BYTE_LENGTH < blockCursor.currentChannel.size())
        {
            blockCursor.filePosition += Block.BYTE_LENGTH;
            readBlock(blockCursor.currentTimeDir, blockCursor.currentChannel, blockCursor, blockCursor.filePosition);
            nextBlock = blockCursor.currentBlock.firstTimestamp() < blockCursor.endTimestamp;
        }
        else
        {
            File nextTimeDir = nextDir(blockCursor.currentTimeDir);
            if (null != nextTimeDir)
            {
                FileChannel nextTimeSeries = PersistentStore.getTimeSeriesChannel(nextTimeDir, READ_ONLY_OPTIONS);
                readBlock(nextTimeDir, nextTimeSeries, blockCursor, 0);
                nextBlock = blockCursor.currentBlock.firstTimestamp() < blockCursor.endTimestamp;
            }
            else
            {
                nextBlock = false;
            }
        }

        return nextBlock;
    }

    public Cursor<Block> query(CharSequence key, long startTimestamp, long endTimestamp)
    {
        return new BlockCursor(key, startTimestamp, endTimestamp);
    }

    public boolean exists(final CharSequence key) throws IOException
    {
        return PersistentStore.getKeyDir(dir, key, false).exists();
    }

    public long lastTimestamp(final String key) throws IOException
    {
        final File keyDir = PersistentStore.getKeyDir(dir, key, false);

        final File lastYear = Dirs.lastInDir(keyDir, VALID_YEARS::contains);
        final File lastMonth = Dirs.lastInDir(lastYear, VALID_MONTHS::contains);
        final File lastDay = Dirs.lastInDir(lastMonth, VALID_DAYS::contains);

        final FileChannel timeSeriesChannel = PersistentStore.getTimeSeriesChannel(lastDay, READ_ONLY_OPTIONS);

        BlockHeader header = BlockHeader.allocateDirect();
        readHeader(header, timeSeriesChannel, timeSeriesChannel.size() - Block.BYTE_LENGTH);

        return header.lastTimestamp();
    }

    private class BlockCursor implements Cursor<Block>
    {
        private final CharSequence key;
        private final long startTimestamp;
        private final long endTimestamp;
        private final Block currentBlock = Block.newDirectBlock();
        private final Set<FileChannel> channelsToClose = new HashSet<>();

        private File currentTimeDir = null;
        private FileChannel currentChannel = null;
        private long filePosition = -1;
        private boolean cursorValid = true;

        public BlockCursor(CharSequence key, long startTimestamp, long endTimestamp)
        {
            this.key = key;
            this.startTimestamp = startTimestamp;
            this.endTimestamp = endTimestamp;
        }

        private void setLocation(File timeDir, FileChannel timeSeriesChannel, long filePosition)
        {
            this.currentTimeDir = timeDir;
            this.currentChannel = timeSeriesChannel;
            this.filePosition = filePosition;

            channelsToClose.add(timeSeriesChannel);
        }

        @Override
        public boolean moveNext() throws IOException
        {
            boolean next = cursorValid;
            if (next)
            {
                if (null == currentTimeDir)
                {
                    next = findCurrentBlock(this);
                }
                else
                {
                    next = nextBlock(this);
                }

                cursorValid = next;
            }

            return next;
        }

        @Override
        public Block current()
        {
            if (!cursorValid)
            {
                throw new IllegalStateException();
            }

            return currentBlock;
        }

        @Override
        public void close()
        {
            channelsToClose.forEach(PersistentStoreReader::close);
            channelsToClose.clear();
        }
    }

    private static void close(AutoCloseable c)
    {
        try
        {
            c.close();
        }
        catch (Exception e)
        {
            // No-op
        }
    }
}
