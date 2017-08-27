package com.titizz.exercise.distributedlock;

import org.junit.Test;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by code4wt on 17/8/27.
 */
public class ZKReadLockTest2 {

    @Test
    public void lock() throws Exception {
        Runnable runnable = () -> {
            try {
                ZKReadWriteLock2 crwl = new ZKReadWriteLock2();
                crwl.readLock().lock();
                Thread.sleep(1000 + new Random(System.nanoTime()).nextInt(2000));
                crwl.readLock().unlock();
            } catch (Exception e) {
                e.printStackTrace();
            }
        };

        int poolSize = 4;
        ExecutorService executorService = Executors.newFixedThreadPool(poolSize);
        for (int i = 0; i < poolSize; i++) {
            executorService.submit(runnable);
        }

        executorService.awaitTermination(10, TimeUnit.SECONDS);
    }

    @Test
    public void tryLock() throws Exception {
        ZKReadWriteLock2 crwl = new ZKReadWriteLock2();
        Boolean locked = crwl.readLock().tryLock();
        System.out.println("locked: " + locked);
        crwl.readLock().unlock();
    }

    @Test
    public void tryLock1() throws Exception {
        ZKReadWriteLock2 crwl = new ZKReadWriteLock2();
        Boolean locked = crwl.readLock().tryLock(20000);
        System.out.println("locked: " + locked);
        crwl.readLock().unlock();
    }

}