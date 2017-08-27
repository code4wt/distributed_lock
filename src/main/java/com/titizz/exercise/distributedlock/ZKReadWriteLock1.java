package com.titizz.exercise.distributedlock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;

import java.util.*;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

/**
 * Created by code4wt on 17/8/26.
 */
public class ZKReadWriteLock1 implements ReadWriteLock {

    private static final String LOCK_NODE_PARENT_PATH = "/share_lock";

    /** 自旋测试超时阈值，考虑到网络的延时性，这里设为1000毫秒 */
    private static final long spinForTimeoutThreshold = 1000L;

    private static final long SLEEP_TIME = 100L;

    private ZooKeeper zooKeeper;

    private CountDownLatch connectedSemaphore = new CountDownLatch(1);

    private ReadLock readLock = new ReadLock();

    private WriteLock writeLock = new WriteLock();

    private Comparator<String> nameComparator;

    public ZKReadWriteLock1() throws Exception {
        Watcher watcher = event -> {
            if (KeeperState.SyncConnected == event.getState()) {
                connectedSemaphore.countDown();
            }
        };
        zooKeeper = new ZooKeeper("127.0.0.1:2181", 1000, watcher);
        connectedSemaphore.await();

        nameComparator = (x, y) -> {
            Integer xs = getSequence(x);
            Integer ys = getSequence(y);
            return xs > ys ? 1 : (xs < ys ? -1 : 0);
        };
    }

    @Override
    public DistributedLock readLock() {
        return readLock;
    }

    @Override
    public DistributedLock writeLock() {
        return writeLock;
    }

    private class ReadLock implements DistributedLock, Watcher {

        private LockStatus lockStatus = LockStatus.UNLOCK;

        private CyclicBarrier lockBarrier = new CyclicBarrier(2);

        private String prefix = new Random(System.nanoTime()).nextInt(10000000) + "-read-";

        private String name;

        @Override
        public void lock() throws Exception {
            if (lockStatus != LockStatus.UNLOCK) {
                return;
            }

            // 1. 创建锁节点
            if (name == null) {
                name = createLockNode(prefix);
                name = name.substring(name.lastIndexOf("/") + 1);
                System.out.println("创建锁节点 " + name);
            }

            // 2. 获取锁节点列表，并设置监视器
            List<String> nodes = zooKeeper.getChildren(LOCK_NODE_PARENT_PATH, this);
            nodes.sort(nameComparator);

            // 3. 检查能否获取锁，若能，直接返回
            if (canAcquireLock(name, nodes)) {
                System.out.println(name + " 获取锁");
                lockStatus = LockStatus.LOCKED;
                return;
            }

            lockStatus = LockStatus.TRY_LOCK;
            lockBarrier.await();
        }

        @Override
        public Boolean tryLock() throws Exception {
            if (lockStatus == LockStatus.LOCKED) {
                return true;
            }

            if (name == null) {
                name = createLockNode(prefix);
                name = name.substring(name.lastIndexOf("/") + 1);
                System.out.println("创建锁节点 " + name);
            }

            List<String> nodes = zooKeeper.getChildren(LOCK_NODE_PARENT_PATH, this);
            nodes.sort(nameComparator);

            if (canAcquireLock(name, nodes)) {
                lockStatus = LockStatus.LOCKED;
                return true;
            }

            return false;
        }

        @Override
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

        @Override
        public void unlock() throws Exception {
            if (lockStatus == LockStatus.UNLOCK) {
                return;
            }

            deleteLockNode(name);
            lockStatus = LockStatus.UNLOCK;
            lockBarrier.reset();
            System.out.println(name + " 释放锁");
            name = null;
        }

        @Override
        public void process(WatchedEvent event) {
            if (KeeperState.SyncConnected != event.getState()) {
                return;
            }

            if (EventType.None == event.getType() && event.getPath() == null) {
                connectedSemaphore.countDown();
            } else if (EventType.NodeChildrenChanged == event.getType()
                    && event.getPath().equals(LOCK_NODE_PARENT_PATH)) {

                if (lockStatus != LockStatus.TRY_LOCK) {
                    return;
                }

                List<String> nodes = null;
                try {
                    // 获取锁列表
                    nodes = zooKeeper.getChildren(LOCK_NODE_PARENT_PATH, this);
                    nodes.sort(nameComparator);
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                    return;
                }

                // 判断前面是否有写操作 ? 获取锁 ：等待
                if (canAcquireLock(name, nodes)) {
                    lockStatus = LockStatus.LOCKED;
                    try {
                        lockBarrier.await();
                        System.out.println(name + " 获取锁");
                    } catch (InterruptedException | BrokenBarrierException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private class WriteLock implements DistributedLock, Watcher {

        private LockStatus lockStatus = LockStatus.UNLOCK;

        private CyclicBarrier lockBarrier = new CyclicBarrier(2);

        private String prefix = new Random(System.nanoTime()).nextInt(1000000) + "-write-";

        private String name;

        @Override
        public void lock() throws Exception {
            if (lockStatus != LockStatus.UNLOCK) {
                return;
            }

            if (name == null) {
                name = createLockNode(prefix);
                name = name.substring(name.lastIndexOf("/") + 1);
                System.out.println("创建锁节点 " + name);
            }

            List<String> nodes = zooKeeper.getChildren(LOCK_NODE_PARENT_PATH, this);
            nodes.sort(nameComparator);

            if (isFirstNode(name, nodes)) {
                System.out.println(name + " 获取锁");
                lockStatus = LockStatus.LOCKED;
                return;
            }

            lockStatus = LockStatus.TRY_LOCK;
            lockBarrier.await();
        }

        @Override
        public Boolean tryLock() throws Exception {
            if (lockStatus == LockStatus.LOCKED) {
                return true;
            }

            if (name == null) {
                name = createLockNode(prefix);
                name = name.substring(name.lastIndexOf("/") + 1);
                System.out.println("创建锁节点 " + name);
            }

            List<String> nodes = zooKeeper.getChildren(LOCK_NODE_PARENT_PATH, this);
            nodes.sort(nameComparator);

            if (isFirstNode(name, nodes)) {
                lockStatus = LockStatus.LOCKED;
                return true;
            }

            return false;
        }

        @Override
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

        @Override
        public void unlock() throws Exception {
            if (lockStatus == LockStatus.UNLOCK) {
                return;
            }

            deleteLockNode(name);
            lockStatus = LockStatus.UNLOCK;
            lockBarrier.reset();
            System.out.println(name + " 释放锁");
            name = null;
        }

        @Override
        public void process(WatchedEvent event) {
            if (KeeperState.SyncConnected != event.getState()) {
                return;
            }

            if (EventType.None == event.getType() && event.getPath() == null) {
                connectedSemaphore.countDown();
            } else if (EventType.NodeChildrenChanged == event.getType()
                    && event.getPath().equals(LOCK_NODE_PARENT_PATH)) {

                if (lockStatus != LockStatus.TRY_LOCK) {
                    return;
                }

                List<String> nodes = null;
                try {
                    // 获取锁列表
                    nodes = zooKeeper.getChildren(LOCK_NODE_PARENT_PATH, this);
                    nodes.sort(nameComparator);
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                    return;
                }

                // 判断前面是否有写操作
                if (isFirstNode(name, nodes)) {
                    lockStatus = LockStatus.LOCKED;
                    try {
                        lockBarrier.await();
                        System.out.println(name + " 获取锁");
                    } catch (InterruptedException | BrokenBarrierException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private String createLockNode(String name) {
        String path = null;
        try {
            path = zooKeeper.create(LOCK_NODE_PARENT_PATH + "/" + name, "".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        } catch (KeeperException | InterruptedException e) {
            System.out.println(" failed to create lock node");
            return null;
        }

        return path;
    }

    private void deleteLockNode(String name) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(LOCK_NODE_PARENT_PATH + "/" + name, false);
        zooKeeper.delete(LOCK_NODE_PARENT_PATH + "/" + name, stat.getVersion());
    }

    private String getPrefix(String name) {
        return name.substring(0, name.lastIndexOf('-') + 1);
    }

    private Integer getSequence(String name) {
        return Integer.valueOf(name.substring(name.lastIndexOf("-") + 1));
    }

    private Boolean canAcquireLock(String name, List<String> nodes) {
        if (isFirstNode(name, nodes)) {
            return true;
        }

        Map<String, Boolean> map = new HashMap<>();
        boolean hasWriteoperation = false;
        for (String n : nodes) {
            if (n.contains("read") && !hasWriteoperation) {
                map.put(n, true);
            } else {
                hasWriteoperation = true;
                map.put((n), false);
            }
        }

        return map.get(name);
    }

    private Boolean isFirstNode(String name, List<String> nodes) {
        return nodes.get(0).equals(name);
    }
}