package com.lmax.utan.store;

import com.lmax.io.Dirs;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static com.lmax.utan.store.BlockGenerator.generateBlockData;
import static org.assertj.core.api.Assertions.assertThat;

public class PersistentStoreWriterTest
{
    private final TimeSeriesSupplier timeSeriesSupplier = new TimeSeriesSupplier(
        1111111, ZonedDateTime.of(2017, 8, 6, 12, 0, 0, 0, ZoneId.of("UTC")).toInstant().toEpochMilli());
    private File dir;
    private String key = "this.is.a,key.to.store";

    @Before
    public void setUp() throws Exception
    {
        dir = Dirs.createTempDir(PersistentStoreWriterTest.class.getSimpleName());
    }

    @Test(expected = IOException.class)
    public void rejectIfTimeSeriesDoesNotExist() throws IOException
    {
        PersistentStoreReader reader = new PersistentStoreReader(dir);
        PersistentStoreWriter writer = new PersistentStoreWriter(dir);

        final Block blockToStore = Block.newHeapBlock();
        generateBlockData(timeSeriesSupplier, blockToStore);

        String key = "foo";
        writer.store(key, blockToStore);

        long l = blockToStore.lastTimestamp();

        ZonedDateTime utc = ZonedDateTime.ofInstant(Instant.ofEpochMilli(l), ZoneId.of("UTC"));
        ZonedDateTime in2DaysTime = utc.plusDays(2);
        long futureTimestamp = in2DaysTime.toInstant().toEpochMilli();

        reader.findBlockContainingTimestamp(key, futureTimestamp);
    }

    @Test
    public void storeAndLoadSingleBlock() throws Exception
    {
        final Block blockToStore = Block.newHeapBlock();
        ArrayList<Entry> entries = new ArrayList<>();
        generateBlockData(timeSeriesSupplier, blockToStore, entries);

        PersistentStoreWriter toStore = new PersistentStoreWriter(dir);
        PersistentStoreReader toLoad = new PersistentStoreReader(dir);

        toStore.store(key, blockToStore);

        Block block = toLoad.findBlockContainingTimestamp(key, timeSeriesSupplier.getBeginTimestamp());

        assertThat(block.firstTimestamp()).isEqualTo(entries.get(0).timestamp);
        assertThat(block.lastTimestamp()).isEqualTo(entries.get(entries.size() - 1).timestamp);
    }

    @Test
    public void findBlockOnNextDay() throws Exception
    {
        final Block block0 = Block.newHeapBlock();
        List<Entry> firstDayEntries = generateBlockData(timeSeriesSupplier, block0, new ArrayList<>());

        final Block block1 = Block.newHeapBlock();
        long futureTimestamp = timestamp2DayInFuture(firstDayEntries);
        TimeSeriesSupplier timeSeriesSupplier = new TimeSeriesSupplier(1234234, futureTimestamp);
        List<Entry> futureDayEntries = generateBlockData(timeSeriesSupplier, block1, new ArrayList<>());

        PersistentStoreWriter toStore = new PersistentStoreWriter(dir);
        PersistentStoreReader toLoad = new PersistentStoreReader(dir);

        toStore.store(key, block0);
        toStore.store(key, block1);

        Block block = toLoad.findBlockContainingTimestamp(key, firstDayEntries.get(firstDayEntries.size() - 1).timestamp + 1);

        assertThat(block).isNotNull();
        assertThat(block.firstTimestamp()).isEqualTo(futureDayEntries.get(0).timestamp);
        assertThat(block.lastTimestamp()).isEqualTo(futureDayEntries.get(futureDayEntries.size() - 1).timestamp);
    }

    @Test
    public void findBlockOnNextDayWithNoDataOnCurrentDay() throws Exception
    {
        final Block block0 = Block.newHeapBlock();
        List<Entry> firstDayEntries = generateBlockData(timeSeriesSupplier, block0, new ArrayList<>());

        final Block block1 = Block.newHeapBlock();
        long futureTimestamp = timestamp2DayInFuture(firstDayEntries);
        TimeSeriesSupplier timeSeriesSupplier = new TimeSeriesSupplier(1234234, futureTimestamp);
        List<Entry> futureDayEntries = generateBlockData(timeSeriesSupplier, block1, new ArrayList<>());

        long midTimestamp = asZonedDateTime(firstDayEntries).plusDays(1).toInstant().toEpochMilli();
        PersistentStoreWriter toStore = new PersistentStoreWriter(dir);
        PersistentStoreReader toLoad = new PersistentStoreReader(dir);

        toStore.store(key, block0);
        toStore.store(key, block1);

        Block block = toLoad.findBlockContainingTimestamp(key, midTimestamp);

        assertThat(block.firstTimestamp()).isEqualTo(futureDayEntries.get(0).timestamp);
        assertThat(block.lastTimestamp()).isEqualTo(futureDayEntries.get(futureDayEntries.size() - 1).timestamp);
    }

    @Test
    public void findBlockOnNextMonth() throws Exception
    {
        final Block block0 = Block.newHeapBlock();
        List<Entry> firstDayEntries = generateBlockData(timeSeriesSupplier, block0, new ArrayList<>());

        final Block block1 = Block.newHeapBlock();
        long futureTimestamp = timestamp1MonthInFuture(firstDayEntries);
        TimeSeriesSupplier timeSeriesSupplier = new TimeSeriesSupplier(1234234, futureTimestamp);
        List<Entry> futureDayEntries = generateBlockData(timeSeriesSupplier, block1, new ArrayList<>());

        PersistentStoreWriter toStore = new PersistentStoreWriter(dir);
        PersistentStoreReader toLoad = new PersistentStoreReader(dir);

        toStore.store(key, block0);
        toStore.store(key, block1);

        Block block = toLoad.findBlockContainingTimestamp(key, firstDayEntries.get(firstDayEntries.size() - 1).timestamp + 1);

        assertThat(block).isNotNull();
        assertThat(block.firstTimestamp()).isEqualTo(futureDayEntries.get(0).timestamp);
        assertThat(block.lastTimestamp()).isEqualTo(futureDayEntries.get(futureDayEntries.size() - 1).timestamp);
    }

    private long timestamp2DayInFuture(List<Entry> firstDayEntries)
    {
        return asZonedDateTime(firstDayEntries).plusDays(2).toInstant().toEpochMilli();
    }

    private long timestamp1MonthInFuture(List<Entry> firstDayEntries)
    {
        return asZonedDateTime(firstDayEntries).plusMonths(1).toInstant().toEpochMilli();
    }

    private ZonedDateTime asZonedDateTime(List<Entry> firstDayEntries)
    {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(firstDayEntries.get(firstDayEntries.size() - 1).timestamp), ZoneId.of("UTC"));
    }
}