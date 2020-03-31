package ru.gnkoshelev.kontur.intern.redis.map;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MultithreadedStressTester {
    private final ExecutorService executor;
    private final int threadCount;
    private final int iterationCount;

    public MultithreadedStressTester(int threadCount, int iterationCount) {
        this.threadCount = threadCount;
        this.iterationCount = iterationCount;
        this.executor = Executors.newCachedThreadPool();
    }

    public void stress(final Runnable action) throws InterruptedException {
        spawnThreads(action).await();
    }

    private CountDownLatch spawnThreads(final Runnable action) {
        final CountDownLatch finished = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            executor.execute(() -> {
                try {
                    repeat(action);
                } finally {
                    finished.countDown();
                }
            });
        }
        return finished;
    }

    private void repeat(Runnable action) {
        for (int i = 0; i < iterationCount; i++) {
            action.run();
        }
    }

    public void shutdown() {
        executor.shutdown();
    }
}