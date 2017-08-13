package com.lmax.utan.store;

import com.lmax.io.Dirs;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

@State(Scope.Benchmark)
public class FSyncBenchmark
{
    @Param({"16", "32", "128"})
    private int numFiles;

    private FileChannel[] fileChannels;
    private ByteBuffer[] data;
    private File dir;

    @Setup
    public void setup() throws IOException
    {
        dir = Dirs.createTempDir(System.getProperty("user.dir"));

        fileChannels = new FileChannel[numFiles];
        data = new ByteBuffer[numFiles];
        for (int i = 0; i < numFiles; i++)
        {
            Path resolve = dir.toPath().resolve("test_" + i);
            fileChannels[i] = FileChannel.open(resolve, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            data[i] = ByteBuffer.allocateDirect(4096);
        }
    }

    @Benchmark
    public void noSync() throws IOException
    {
        for (int i = 0, n = this.numFiles; i < n; i++)
        {
            final ByteBuffer datum = data[i];
            datum.clear();
            final long aLong = datum.getLong(2048);
            datum.putLong(2048, aLong + 1);

            fileChannels[i].write(datum, 0);
        }
    }

    @Benchmark
    public void syncEach() throws IOException
    {
        for (int i = 0, n = this.numFiles; i < n; i++)
        {
            final ByteBuffer datum = data[i];
            datum.clear();
            final long aLong = datum.getLong(2048);
            datum.putLong(2048, aLong + 1);

            fileChannels[i].write(datum, 0);
            fileChannels[i].force(true);
        }
    }

    @Benchmark
    public void syncInOrder() throws IOException
    {
        for (int i = 0, n = this.numFiles; i < n; i++)
        {
            final ByteBuffer datum = data[i];
            datum.clear();
            final long aLong = datum.getLong(2048);
            datum.putLong(2048, aLong + 1);

            fileChannels[i].write(datum, 0);
        }

        for (int i = 0, n = this.numFiles; i < n; i++)
        {
            fileChannels[i].force(true);
        }
    }

    @Benchmark
    public void syncReverseOrder() throws IOException
    {
        for (int i = 0, n = this.numFiles; i < n; i++)
        {
            final ByteBuffer datum = data[i];
            datum.clear();
            final long aLong = datum.getLong(2048);
            datum.putLong(2048, aLong + 1);

            fileChannels[i].write(datum, 0);
        }

        for (int i = this.numFiles; --i != -1;)
        {
            fileChannels[i].force(true);
        }
    }
}
