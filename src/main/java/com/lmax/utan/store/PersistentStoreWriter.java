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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.lmax.utan.io.Dirs.ensureDirExists;
import static java.lang.ThreadLocal.withInitial;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class PersistentStoreWriter
{
    private static final long BLOCK_ALREADY_FROZEN = -1;
    private static final long BLOCK_OLDER_THAN_EXISTING = -2;
    private final static Set<? extends OpenOption> READ_WRITE_OPTIONS = EnumSet.of(CREATE, READ, WRITE);
    private final Map<String, File> keyDirCache = new HashMap<>();

    private final File dir;

    private final ThreadLocal<Block> currentBlock = withInitial(
        () -> new Block(new UnsafeBuffer(ByteBuffer.allocateDirect(Block.BYTE_LENGTH))));

    public PersistentStoreWriter(File dir) throws IOException
    {
        ensureDirExists(dir);

        this.dir = dir;
    }

    public File keyDir(String key)
    {
        byte[] keyAsBytes = key.getBytes(StandardCharsets.UTF_8);

        try
        {
            File keyDir = PersistentStore.getKeyDir(this.dir, keyAsBytes, true);
            ensureKeyFileExists(keyDir, keyAsBytes);

            return keyDir;
        }
        catch (IOException e)
        {
            throw new RuntimeException("Failed to create directory for key: " + key, e);
        }
    }

    public void store(CharSequence key, Block block) throws IOException
    {
        File keyDir = keyDirCache.computeIfAbsent(key.toString(), this::keyDir);
        File timeDir = PersistentStore.getTimeDir(keyDir, block.firstTimestamp(), true);

        long writePosition = -1;
        try (FileChannel timeSeries = PersistentStore.getTimeSeriesChannel(timeDir, READ_WRITE_OPTIONS))
        {
            writePosition = getWritePosition(timeSeries, block);

            if (writePosition == -1)
            {
                throw new IOException("Tried to overwrite already frozen block");
            }
            else if (writePosition == -2)
            {
                throw new IOException("Tried to write block with earlier timestamp");
            }

            block.underlyingBuffer().clear();
            timeSeries.write(block.underlyingBuffer(), writePosition);
        }
        catch (Exception e)
        {
            final String message = "Failed to write block - name: " + key + ", dir: " + timeDir + ", block: " + block + ", writePosition: " + writePosition;
            throw new IOException(message, e);
        }
    }

    private long getWritePosition(FileChannel timeSeries, Block incomingBlock) throws IOException
    {
        if (timeSeries.size() == 0)
        {
            return 0;
        }

        // TODO: Use block header
        Block storedBlock = currentBlock.get();
        // Read last block.
        storedBlock.underlyingBuffer().clear();
        timeSeries.read(storedBlock.underlyingBuffer(), timeSeries.size() - Block.BYTE_LENGTH);

        if (incomingBlock.firstTimestamp() == storedBlock.firstTimestamp() && storedBlock.isFrozen())
        {
            throw new IOException("incoming: " + incomingBlock + ", stored: " + storedBlock);
        }

        if (incomingBlock.firstTimestamp() < storedBlock.firstTimestamp())
        {
            return BLOCK_OLDER_THAN_EXISTING;
        }

        return storedBlock.isFrozen() ? timeSeries.size() : timeSeries.size() - Block.BYTE_LENGTH;
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
