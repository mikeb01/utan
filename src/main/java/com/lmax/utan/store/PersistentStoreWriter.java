package com.lmax.utan.store;

import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.OpenOption;
import java.util.EnumSet;
import java.util.Set;

import static com.lmax.io.Dirs.ensureDirExists;
import static java.lang.ThreadLocal.withInitial;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.agrona.BitUtil.toHex;

public class PersistentStoreWriter
{
    private static final long BLOCK_ALREADY_FROZEN = -1;
    private static final long BLOCK_OLDER_THAN_EXISTING = -2;
    private final static Set<? extends OpenOption> READ_WRITE_OPTIONS = EnumSet.of(CREATE, READ, WRITE);

    private final File dir;

    private final ThreadLocal<Block> currentBlock = withInitial(
        () -> new Block(new UnsafeBuffer(ByteBuffer.allocateDirect(Block.BYTE_LENGTH))));

    public PersistentStoreWriter(File dir) throws IOException
    {
        ensureDirExists(dir);

        this.dir = dir;
    }

    public void store(CharSequence key, Block block) throws IOException
    {
        byte[] keyAsBytes = key.toString().getBytes(StandardCharsets.UTF_8);

        File keyDir = PersistentStore.getKeyDir(this.dir, keyAsBytes, true);
        ensureKeyFileExists(keyDir, keyAsBytes);

        File timeDir = PersistentStore.getTimeDir(keyDir, block.firstTimestamp(), true);

        FileChannel timeSeries = PersistentStore.getTimeSeriesChannel(timeDir, READ_WRITE_OPTIONS);

        long writePosition = getWritePosition(timeSeries, block.firstTimestamp());
        ByteBuffer src = block.underlyingBuffer();
        src.position(0).limit(Block.BYTE_LENGTH);

        timeSeries.write(src, writePosition);
    }

    private long getWritePosition(FileChannel timeSeries, long incomingFirstTimestamp) throws IOException
    {
        if (timeSeries.size() == 0)
        {
            return 0;
        }

        Block block = currentBlock.get();
        // Read last block.
        timeSeries.read(block.underlyingBuffer(), timeSeries.size() - Block.BYTE_LENGTH);

        if (incomingFirstTimestamp == block.firstTimestamp() && block.isFrozen())
        {
            return BLOCK_ALREADY_FROZEN;
        }

        if (incomingFirstTimestamp < block.firstTimestamp())
        {
            return BLOCK_OLDER_THAN_EXISTING;
        }

        return block.isFrozen() ? timeSeries.size() : timeSeries.size() - Block.BYTE_LENGTH;
    }

    private void ensureKeyFileExists(File keyPath, byte[] key) throws IOException
    {
        File keyFile = new File(keyPath, "key.txt");
        if (!keyFile.exists())
        {
            File tmpKeyFile = new File(keyPath, "key.txt.tmp");
            FileOutputStream out = new FileOutputStream(tmpKeyFile);
            out.write(key);
            out.getFD().sync();
            if (!tmpKeyFile.renameTo(keyFile))
            {
                throw new IOException("Unable to create key file: " + keyPath);
            }
        }
    }

}
