package com.titizz.exercise.distributedlock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

/**
 * Created by code4wt on 17/8/24.
 */
public class ExclusiveLock implements DistributedLock {

    private static final Logger logger = LoggerFactory.getLogger(ExclusiveLock.class);

    private static final String LOCK_NODE_FULL_PATH = "/exclusive_lock/lock";

    /** 自旋测试超时阈值，考虑到网络的延时性，这里设为1000毫秒 */
    private static final long spinForTimeoutThreshold = 1000L;

    private static final long SLEEP_TIME = 100L;

    private ZooKeeper zooKeeper;

    private CountDownLatch connectedSemaphore = new CountDownLatch(1);

    private CyclicBarrier lockBarrier = new CyclicBarrier(2);

    private LockStatus lockStatus;

    private String id = String.valueOf(new Random(System.nanoTime()).nextInt(10000000));

    public ExclusiveLock() throws InterruptedException, IOException {
        zooKeeper = new ZooKeeper("127.0.0.1:2181", 1000, new LockNodeWatcher());
        lockStatus = LockStatus.UNLOCK;
        connectedSemaphore.await();
    }

    public void lock() throws Exception {
        if (lockStatus != LockStatus.UNLOCK) {
            return;
        }

        // 1. 创建锁节点
        if (createLockNode()) {
            System.out.println("[" + id  + "]" + " 获取锁");
            lockStatus = LockStatus.LOCKED;
            return;
        }

        lockStatus = LockStatus.TRY_LOCK;
        lockBarrier.await();
    }

    public Boolean tryLock() {
        if (lockStatus == LockStatus.LOCKED) {
            return true;
        }

        Boolean created = createLockNode();
        lockStatus = created ? LockStatus.LOCKED : LockStatus.UNLOCK;
        return created;
    }

    public Boolean tryLock(long millisecond) throws Exception {
        long millisTimeout = millisecond;
        if (millisTimeout <= 0L) {
            return false;
        }

        final long deadline = System.currentTimeMillis() + millisTimeout;
        for (;;) {
            if (tryLock()) {
                return true;
            }

            if (millisTimeout > spinForTimeoutThreshold) {
                Thread.sleep(SLEEP_TIME);
            }

            millisTimeout = deadline - System.currentTimeMillis();
            if (millisTimeout <= 0L) {
                return false;
            }
        }
    }

    public void unlock() throws Exception {
        if (lockStatus == LockStatus.UNLOCK) {
            return;
        }

        deleteLockNode();
        lockStatus = LockStatus.UNLOCK;
        lockBarrier.reset();
        System.out.println("[" + id  + "]" + " 释放锁");
    }

    private Boolean createLockNode() {
        try {
            zooKeeper.create(LOCK_NODE_FULL_PATH, "".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (KeeperException | InterruptedException e) {
            return false;
        }

        return true;
    }

    private void deleteLockNode() throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(LOCK_NODE_FULL_PATH, false);
        zooKeeper.delete(LOCK_NODE_FULL_PATH, stat.getVersion());
    }

    enum LockStatus {
        TRY_LOCK,
        LOCKED,
        UNLOCK
    }

    class LockNodeWatcher implements Watcher {

        public void process(WatchedEvent event) {
            if (KeeperState.SyncConnected != event.getState()) {
                return;
            }

            // 2. 设置监视器
            try {
                zooKeeper.exists(LOCK_NODE_FULL_PATH, this);
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }

            if (EventType.None == event.getType() && event.getPath() == null) {
                connectedSemaphore.countDown();
            } else if (EventType.NodeDeleted == event.getType()
                    && event.getPath().equals(LOCK_NODE_FULL_PATH)) {

                // 3. 再次尝试创建锁及诶单
                if (lockStatus == LockStatus.TRY_LOCK && createLockNode()) {
                    lockStatus = LockStatus.LOCKED;
                    try {
                        lockBarrier.await();
                        System.out.println("[" + id  + "]" + " 获取锁");
                        return;
                    } catch (InterruptedException | BrokenBarrierException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
