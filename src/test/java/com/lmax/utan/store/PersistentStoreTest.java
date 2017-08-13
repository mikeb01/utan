package com.lmax.utan.store;

import com.lmax.io.Dirs;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static com.lmax.utan.store.BlockGenerator.generateBlockData;
import static junit.framework.TestCase.fail;
import static org.assertj.core.api.Assertions.assertThat;

public class PersistentStoreTest
{
    private final TimeSeriesSupplier timeSeriesSupplier = new TimeSeriesSupplier(1111111, System.currentTimeMillis());
    private File dir;

    @Before
    public void setUp() throws Exception
    {
        dir = Dirs.createTempDir(PersistentStoreTest.class.getSimpleName());
    }

    private static class BasicSubscriber implements Subscriber<Block>
    {
        private final List<Block> blocks = new ArrayList<>();
        private boolean completed;

        @Override
        public void onSubscribe(Subscription s)
        {
            s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(Block block)
        {
            blocks.add(block);
        }

        @Override
        public void onError(Throwable t)
        {
            fail();
        }

        @Override
        public void onComplete()
        {
            this.completed = true;
        }
    }
}