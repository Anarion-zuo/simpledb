package simpledb;

import org.junit.Assert;
import org.junit.Test;
import simpledb.common.DbException;
import simpledb.storage.HeapPageId;
import simpledb.storage.PageId;
import simpledb.transaction.LockManager;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionId;

import java.util.ArrayList;

public class LockManagerTest {

    private final LockManager lockManager = new LockManager();

    @Test public void sharedLockTest() {
        TransactionId tid1 = new TransactionId();
        TransactionId tid2 = new TransactionId();
        PageId pageId = new HeapPageId(0, 0);
        lockManager.aquireSharedLock(tid1, pageId);
        // should not block
        lockManager.aquireSharedLock(tid2, pageId);
        try {
            lockManager.releaseSharedLock(tid1, pageId);
            lockManager.releaseSharedLock(tid2, pageId);
        } catch (DbException e) {
            Assert.fail();
        }
        try {
            lockManager.releaseSharedLock(tid1, pageId);
            Assert.fail("releasing not aquired lock not throwing");
        } catch (DbException e) {

        }
        try {
            lockManager.releaseSharedLock(tid2, pageId);
            Assert.fail("releasing not aquired lock not throwing");
        } catch (DbException e) {

        }
    }

    @Test public void exclusiveLockTest() {
        TransactionId tid1 = new TransactionId();
        PageId pageId = new HeapPageId(0, 0);
        lockManager.aquireExclusiveLock(tid1, pageId);
        // same lock can be aquired twice by the same transaction
        // but not by any other transaction
        lockManager.aquireExclusiveLock(tid1, pageId);
        // can use an exclusive lock as a shared lock
        lockManager.aquireSharedLock(tid1, pageId);
        try {
            lockManager.releaseSharedLock(tid1, pageId);
            Assert.fail("release exclusive lock as shared lock");
        } catch (DbException e) {
        }
        try {
            lockManager.releaseExclusiveLock(tid1, pageId);
        } catch (DbException e) {
            Assert.fail("cannot release aquired lock");
        }
        try {
            lockManager.releaseExclusiveLock(tid1, pageId);
            Assert.fail("releasing not aquired lock not throwing");
        } catch (DbException e) {

        }
    }

    @Test public void upgradeLockTest() {
        TransactionId tid1 = new TransactionId();
        PageId pageId = new HeapPageId(0, 0);
        lockManager.aquireSharedLock(tid1, pageId);
        // should not block
        lockManager.aquireSharedLock(tid1, pageId);
        // try upgrade
        lockManager.aquireExclusiveLock(tid1, pageId);
        // cannot release shared
        try {
            lockManager.releaseSharedLock(tid1, pageId);
            Assert.fail("upgraded shared lock must not be released as a shared lock");
        } catch (DbException e) {

        }
        // can be released as exclusive lock
        try {
            lockManager.releaseExclusiveLock(tid1, pageId);
        } catch (DbException e) {
            Assert.fail("upgraded shared lock cannot be released as exclusive lock");
        }
        // cannot release twice
        try {
            lockManager.releaseExclusiveLock(tid1, pageId);
            Assert.fail("releasing not aquired lock not throwing");
        } catch (DbException e) {

        }
    }

    @Test public void sharedAfterExclusiveTest() throws DbException {
        TransactionId tid1 = new TransactionId();
        PageId pageId = new HeapPageId(0, 0);
        // exclusive lock first
        lockManager.aquireExclusiveLock(tid1, pageId);
        // then shared lock
        // must not block
        lockManager.aquireSharedLock(tid1, pageId);
        // cannot release as shared lock
        try {
            lockManager.releaseSharedLock(tid1, pageId);
            Assert.fail("release exclusive lock as shared lock");
        } catch (DbException e) {

        }
        // can release as exclusive lock
        lockManager.releaseExclusiveLock(tid1, pageId);
    }

    @Test public void exclusiveWaitsForSharedTest() throws InterruptedException {
        TransactionId tid1 = new TransactionId();
        PageId pageId = new HeapPageId(0, 0);
        lockManager.aquireSharedLock(tid1, pageId);
        var ref = new Object() {
            boolean flag = false;
        };
        // start a thread to aquire exclusive lock
        Thread thread = new Thread(() -> {
            TransactionId tid2 = new TransactionId();
            // should block
            lockManager.aquireExclusiveLock(tid2, pageId);
            if (!ref.flag) {
                Assert.fail("exclusive lock did not wait for shared lock");
            }
        });
        thread.start();
        Thread.sleep(500);  // 500ms
        try {
            ref.flag = true;
            lockManager.releaseSharedLock(tid1, pageId);
        } catch (DbException e) {
            Assert.fail("failed to release aquired shared lock");
        }
        thread.join();
    }

    @Test public void upgradeWaitsForSharedTest() throws InterruptedException {
        TransactionId tid1 = new TransactionId();
        PageId pageId = new HeapPageId(0, 0);
        lockManager.aquireSharedLock(tid1, pageId);
        var ref = new Object() {
            boolean flag = false;
        };
        // start a thread to aquire exclusive lock
        Thread thread = new Thread(() -> {
            TransactionId tid2 = new TransactionId();
            // should block
            lockManager.aquireSharedLock(tid2, pageId);
            lockManager.aquireExclusiveLock(tid2, pageId);
            if (!ref.flag) {
                Assert.fail("exclusive lock did not wait for shared lock");
            }
        });
        thread.start();
        Thread.sleep(500);  // 500ms
        try {
            ref.flag = true;
            lockManager.releaseSharedLock(tid1, pageId);
        } catch (DbException e) {
            Assert.fail("failed to release aquired shared lock");
        }
        thread.join();
    }

    @Test public void sharedWaitsForExclusiveTest() throws InterruptedException {
        TransactionId tid1 = new TransactionId();
        PageId pageId = new HeapPageId(0, 0);
        lockManager.aquireExclusiveLock(tid1, pageId);
        var ref = new Object() {
            boolean flag = false;
        };
        // start a thread to aquire shared lock
        Thread thread = new Thread(() -> {
            TransactionId tid2 = new TransactionId();
            // should block
            lockManager.aquireSharedLock(tid2, pageId);
            if (!ref.flag) {
                Assert.fail("shared lock did not wait for exclusive lock");
            }
        });
        thread.start();
        Thread.sleep(500);  // 500ms
        try {
            ref.flag = true;
            lockManager.releaseExclusiveLock(tid1, pageId);
        } catch (DbException e) {
            Assert.fail("failed to release aquired exclusive lock");
        }
        thread.join();
    }

    @Test public void exclusiveWaitsForExclusiveTest() throws InterruptedException {
        TransactionId tid1 = new TransactionId();
        PageId pageId = new HeapPageId(0, 0);
        lockManager.aquireExclusiveLock(tid1, pageId);
        var ref = new Object() {
            boolean flag = false;
        };
        // start a thread to aquire exclusive lock
        Thread thread = new Thread(() -> {
            TransactionId tid2 = new TransactionId();
            // should block
            lockManager.aquireExclusiveLock(tid2, pageId);
            if (!ref.flag) {
                Assert.fail("exclusive lock did not wait for exlusive lock");
            }
        });
        thread.start();
        Thread.sleep(500);  // 500ms
        try {
            ref.flag = true;
            lockManager.releaseExclusiveLock(tid1, pageId);
        } catch (DbException e) {
            Assert.fail("failed to release aquired exclusive lock");
        }
        thread.join();
    }

    @Test public void exclusiveWaitsForManySharedTest() throws InterruptedException {
        int sharedCount = 1001;
        PageId pageId = new HeapPageId(0, 0);
        ArrayList<TransactionId> sharedIds = new ArrayList<>();
        for (int i = 0; i < sharedCount; ++i) {
            var tid = new TransactionId();
            sharedIds.add(tid);
            lockManager.aquireSharedLock(tid, pageId);
        }
        var ref = new Object() {
            int releasedCount = 0;
        };
        // start a thread to aquire exclusive lock
        Thread thread = new Thread(() -> {
            TransactionId tid2 = new TransactionId();
            // should block
            lockManager.aquireExclusiveLock(tid2, pageId);
            if (ref.releasedCount != sharedCount) {
                Assert.fail("exclusive lock did not wait for all shared locks");
            }
        });
        thread.start();
        try {
            for (var tid : sharedIds) {
                Thread.sleep(1);  // ms
                lockManager.releaseSharedLock(tid, pageId);
                ref.releasedCount++;
            }
        } catch (DbException e) {
            Assert.fail("failed to release aquired shared lock");
        }
        thread.join();
    }

    @Test public void sharedWaitsForPendingExclusiveTest() throws InterruptedException, DbException {
        int sharedCount = 1001;
        PageId pageId = new HeapPageId(0, 0);
        // aquire some shared locks
        TransactionId shared1 = new TransactionId();
        TransactionId shared2 = new TransactionId();
        lockManager.aquireSharedLock(shared1, pageId);
        lockManager.aquireSharedLock(shared2, pageId);

        var prevReleasedWrapper = new Object() {
            boolean prevSharedReleased = false;
            boolean prevExReleased = false;
        };

        // start a thread to aquire exlusive lock
        Thread pendingExclusive = new Thread(() -> {
            TransactionId ex1 = new TransactionId();
            // should block
            lockManager.aquireExclusiveLock(ex1, pageId);
            System.out.println("supposed pending exclusive locks aquired");
            Assert.assertTrue(prevReleasedWrapper.prevSharedReleased);
            // start a thread to aquire shared lock
            Thread tryLockShare = new Thread(() -> {
                TransactionId tryShare = new TransactionId();
                // should block
                lockManager.aquireSharedLock(tryShare, pageId);
                System.out.println("supposed pending shared locks aquired");
                Assert.assertTrue(prevReleasedWrapper.prevSharedReleased);
                try {
                    System.out.println("release supposed pending shared locks");
                    lockManager.releaseSharedLock(tryShare, pageId);
                } catch (DbException e) {
                    Assert.fail("failed to released previously aquired shared lock");
                }
            });
            System.out.println("start pending shared lock");
            tryLockShare.start();
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            try {
                prevReleasedWrapper.prevExReleased = true;
                System.out.println("release exclusive lock");
                lockManager.releaseExclusiveLock(ex1, pageId);
            } catch (DbException e) {
                Assert.fail("failed to released previously aquired exclusive lock");
            }
            try {
                tryLockShare.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        System.out.println("start pending exclusive lock");
        pendingExclusive.start();
        Thread.sleep(500);
        prevReleasedWrapper.prevSharedReleased = true;
        System.out.println("release first shared locks");
        lockManager.releaseSharedLock(shared1, pageId);
        lockManager.releaseSharedLock(shared2, pageId);
        pendingExclusive.join();
    }
}
