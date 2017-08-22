package com.lmax.utan.store;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.LockSupport;

public class ConcurrentStore
{
    private final Map<String, BlockQueue> blocks = new HashMap<>();
    private final Queue<BlockQueue> controlQ = new ConcurrentLinkedQueue<>();
    private final Queue<Block> recycledBlockQ = new ConcurrentLinkedQueue<>();
    private final File dir;
    private final Thread thread;
    private final BackgroundWriter backgroundWriter;

    public ConcurrentStore(File dir, ThreadFactory threadFactory) throws IOException
    {
        this.dir = dir;
        this.backgroundWriter = new BackgroundWriter(dir, controlQ);
        thread = threadFactory.newThread(backgroundWriter);
        thread.setName("ConcurrentStore-backgroundWriter");
    }

    public void start()
    {
        thread.start();
    }

    public void stopAndWait() throws InterruptedException
    {
        thread.interrupt();
        thread.join();
    }

    public void append(String key, long timestamp, double value)
    {
        BlockQueue blockQueue = blocks.get(key);

        if (null == blockQueue)
        {
            blockQueue = new BlockQueue(key);
            blocks.put(key, blockQueue);
            controlQ.add(blockQueue);
        }

        final Block.AppendStatus status = blockQueue.current.append(timestamp, value);

        switch (status)
        {
            case OK:
                break;
            case FULL:
                blockQueue.current.freeze();
                blockQueue.newBlock().append(timestamp, value);
                break;
            case FROZEN:
                throw new IllegalStateException();
        }
    }

    private static class BlockQueue
    {
        private final String key;
        private final Queue<Block> blocks = new ConcurrentLinkedQueue<>();
        private Block current;
        private boolean live = true;

        private BlockQueue(String key)
        {
            this.key = key;
            newBlock();
        }

        public Block newBlock()
        {
            current = Block.newDirectBlock();
            blocks.add(current);
            return current;
        }

        public Block peek()
        {
            return blocks.peek();
        }

        public void pop()
        {
            blocks.poll();
        }
    }

    private static class WriterInfo
    {
        private final BlockQueue blockQueue;
        private long lastWrittenTimestamp = 0;

        private WriterInfo(BlockQueue blockQueue)
        {
            this.blockQueue = blockQueue;
        }

        public Block getChangedBlockForWriting()
        {
            Block block = blockQueue.peek();
            return null != block && block.lastTimestamp() > lastWrittenTimestamp ? block : null;
        }

        public void updateLastWrite(long timestamp)
        {
            lastWrittenTimestamp = timestamp;
        }
    }

    private static class BackgroundWriter implements Runnable
    {
        private final Map<String, WriterInfo> blocks = new HashMap<>();
        private final File dir;
        private final Queue<BlockQueue> controlQ;
        private final PersistentStoreWriter writer;

        public BackgroundWriter(File dir, Queue<BlockQueue> controlQ) throws IOException
        {
            this.dir = dir;
            this.controlQ = controlQ;
            this.writer = new PersistentStoreWriter(dir);
        }

        @Override
        public void run()
        {
            while (!Thread.currentThread().isInterrupted())
            {
                pollControlQ();
                pollBlockQueues();
                LockSupport.parkNanos(1);
            }
        }

        private void pollBlockQueues()
        {
            final Block localBlockCopy = Block.newDirectBlock();

            blocks.forEach(
                (ignore, writerInfo) ->
                {
                    try
                    {
                        boolean pollNext = false;
                        do
                        {
                            Block blockToWrite = writerInfo.getChangedBlockForWriting();

                            if (blockToWrite != null)
                            {
                                blockToWrite.copyTo(localBlockCopy);

                                writer.store(writerInfo.blockQueue.key, localBlockCopy);
                                writerInfo.updateLastWrite(localBlockCopy.lastTimestamp());

                                pollNext = localBlockCopy.isFrozen();
                                if (pollNext)
                                {
                                    writerInfo.blockQueue.pop();
                                }
                            }
                        }
                        while (pollNext);
                    }
                    catch (IOException e)
                    {
                        throw new RuntimeException(e);
                    }
                });
        }

        private void pollControlQ()
        {
            BlockQueue bq;
            while ((bq = controlQ.poll()) != null)
            {
                if (bq.live)
                {
                    if (!blocks.containsKey(bq.key))
                    {
                        blocks.put(bq.key, new WriterInfo(bq));
                    }
                }
                else
                {
                    blocks.remove(bq.key);
                }
            }
        }
    }
}
