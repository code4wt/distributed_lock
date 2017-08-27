package com.titizz.exercise.distributedlock;

import org.junit.Test;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by code4wt on 17/8/27.
 */
public class ZKWriteLockTest2 {
    @Test
    public void lock() throws Exception {
        Runnable runnable = () -> {
            try {
                ZKReadWriteLock2 crwl = new ZKReadWriteLock2();
                crwl.writeLock().lock();
                Thread.sleep(1000 + new Random(System.nanoTime()).nextInt(2000));
                crwl.writeLock().unlock();
            } catch (Exception e) {
                e.printStackTrace();
            }
        };

        int poolSize = 3;
        ExecutorService executorService = Executors.newFixedThreadPool(poolSize);
        for (int i = 0; i < poolSize; i++) {
            executorService.submit(runnable);
            Thread.sleep(10);
        }

        executorService.awaitTermination(10, TimeUnit.SECONDS);
    }

    @Test
    public void tryLock() throws Exception {
        ZKReadWriteLock2 crwl = new ZKReadWriteLock2();
        Boolean locked = crwl.writeLock().tryLock();
        System.out.println("locked: " + locked);
        crwl.writeLock().unlock();
    }

    @Test
    public void tryLock1() throws Exception {
    }
}