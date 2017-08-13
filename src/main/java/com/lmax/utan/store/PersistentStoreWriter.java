package com.lmax.utan.store;

import com.lmax.collection.Sets;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.OpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashSet;
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

    private final ThreadLocal<Block> currentBlock = withInitial(
        () -> new Block(new UnsafeBuffer(ByteBuffer.allocateDirect(Block.BYTE_LENGTH))));

    private final Set<? extends OpenOption> options = Sets.setOf(new HashSet<>(), CREATE, READ, WRITE);

    public PersistentStoreWriter(File dir) throws IOException
    {
        ensureDirExists(dir);

        this.dir = dir;
    }

    public void store(CharSequence key, Block block) throws IOException
    {
        byte[] keyAsBytes = key.toString().getBytes(StandardCharsets.UTF_8);

        File keyDir = getKeyDir(keyAsBytes, true);
        ensureKeyFileExists(keyDir, keyAsBytes);

        File timeDir = getTimeDir(keyDir, block.firstTimestamp(), true);

        FileChannel timeSeries = getChannel(timeDir);

        long writePosition = getWritePosition(timeSeries, block.firstTimestamp());
        ByteBuffer src = block.underlyingBuffer();
        src.position(0).limit(Block.BYTE_LENGTH);

        timeSeries.write(src, writePosition);
    }

    private File getKeyDir(byte[] keyAsBytes, boolean createIfNotExists) throws IOException
    {
        return PersistentStore.getKeyDir(keyAsBytes, createIfNotExists, messageDigestLocal.get(), this.dir);
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

    private FileChannel getChannel(File timeDir) throws IOException
    {
        File timeSeriesFile = new File(timeDir, "timeseries.dat");
        return FileChannel.open(timeSeriesFile.toPath(), options);
    }

    private File getTimeDir(File keyDir, long firstTimestamp, boolean createIfNotExists) throws IOException
    {
        OffsetDateTime offsetDateTime = Instant.ofEpochMilli(firstTimestamp).atOffset(ZoneOffset.UTC);
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

    private static String lPad2(int value)
    {
        String pad = value < 10 ? "0" : "";
        return pad + value;
    }

}
