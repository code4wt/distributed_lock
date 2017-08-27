package com.titizz.exercise.distributedlock;

/**
 * Created by code4wt on 17/8/26.
 */
public interface DistributedLock {

    void lock() throws Exception;
    Boolean tryLock() throws Exception;
    Boolean tryLock(long millisecond) throws Exception;
    void unlock() throws Exception;
}
