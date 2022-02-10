package simpledb.transaction;

import simpledb.common.DbException;
import simpledb.storage.PageId;

import java.util.*;
import java.util.concurrent.locks.*;

public class LockManager {

    private class LockItem {

        private final Condition cond;
        private final Lock lock;
        /**
         * Documents all running reading transactions.
         * Appending reading transactions are not documented.
         */
        private final HashSet<TransactionId> sharedTransactions = new HashSet<>();
        /**
         * Documents the transaction waiting for or currently is writing.
         * If there are reading transactions running,
         *      a writing one must wait for them to release the lock.
         * If there is a writing transaction waiting or running,
         *      all new reading or writing transactions must wait for it to release the lock.
         */
        private TransactionId exclusiveTransaction = null;

        public LockItem() {
            lock = new ReentrantLock();
            cond = lock.newCondition();
        }

        private void throwIfDeadLocked(TransactionId source) throws TransactionAbortedException {
            if (getWaitNodeByTransactionId(source).checkCycleFromThis()) {
                throw new TransactionAbortedException();
            }
        }

        public void sharedLock(TransactionId transactionId) throws TransactionAbortedException {
            assert transactionId != null;
            lock.lock();
            try {
                // can use an exclusive lock as a shared lock
                if (transactionId.equals(exclusiveTransaction)) {

                } else {
                    if (sharedTransactions.contains(transactionId)) {
                        /**
                         * If a shared lock is already attained by the transaction,
                         * do nothing new.
                         */
                    } else {
                        /**
                         * Must wait for the following:
                         * - writing transaction to release the lock.
                         * - appending writing transaction to run and release the lock.
                         * When an exclusive transaction is pending or running,
                         * new shared transactions hangs here.
                         */
                        while (exclusiveTransaction != null) {
                            getWaitNodeByTransactionId(transactionId).addWait(exclusiveTransaction);
                            throwIfDeadLocked(exclusiveTransaction);
                            cond.await();
                        }
                        sharedTransactions.add(transactionId);
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                lock.unlock();
            }
        }

        public void releaseShared(TransactionId transactionId) throws DbException {
            assert transactionId != null;
            lock.lock();
            if (!sharedTransactions.contains(transactionId)) {
                lock.unlock();
                throw new DbException("releasing shared lock without aquiring");
            }
            sharedTransactions.remove(transactionId);
            getWaitNodeByTransactionId(transactionId).releaseThis();
            if (sharedTransactions.isEmpty()) {
                cond.signalAll();
            }
            lock.unlock();
        }

        public void exclusiveLock(TransactionId transactionId) throws TransactionAbortedException {
            assert transactionId != null;
            lock.lock();
            try {
                /**
                 * First, must ensure exclusive Transaction be given to this transactionId.
                 * Must wait for the preceding one to release.
                 */
                while (!transactionId.equals(exclusiveTransaction)) {
                    while (exclusiveTransaction != null) {
                        getWaitNodeByTransactionId(transactionId).addWait(exclusiveTransaction);
                        throwIfDeadLocked(exclusiveTransaction);
                        cond.await();
                    }
                    exclusiveTransaction = transactionId;
                }
                /**
                 * Then, check whether this is an upgrade.
                 * Not neccessary to check for contains first.
                 */
                sharedTransactions.remove(transactionId);
                /**
                 * Then, must wait for all shared locks to be released.
                 */
                while (!sharedTransactions.isEmpty()) {
                    getWaitNodeByTransactionId(transactionId).addWait(sharedTransactions);
                    throwIfDeadLocked(exclusiveTransaction);
                    cond.await();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                lock.unlock();
            }
        }

        public void releaseExclusive(TransactionId transactionId) throws DbException {
            assert transactionId != null;
            lock.lock();
            if (exclusiveTransaction == null || !exclusiveTransaction.equals(transactionId)) {
                lock.unlock();
                throw new DbException("releasing exclusive lock without aquiring");
            }
            getWaitNodeByTransactionId(transactionId).releaseThis();
            exclusiveTransaction = null;
            cond.signalAll();
            lock.unlock();
        }

        public boolean isSharedLocked(TransactionId transactionId) {
            lock.lock();
            boolean ret = sharedTransactions.contains(transactionId);
            lock.unlock();
            return ret;
        }

        public boolean isExclusiveLocked(TransactionId transactionId) {
            lock.lock();
            boolean ret = transactionId.equals(exclusiveTransaction);
            lock.unlock();
            return ret;
        }

        public boolean isLocked(TransactionId transactionId) {
            lock.lock();
            boolean ret = sharedTransactions.contains(transactionId) || transactionId.equals(exclusiveTransaction);
            lock.unlock();
            return ret;
        }

        /**
         * Release whatever lock the thread attained.
         */
        public void tryReleaseLock(TransactionId transactionId) {
            lock.lock();
            if (sharedTransactions.contains(transactionId)) {
                // must be shared locked
                sharedTransactions.remove(transactionId);
                getWaitNodeByTransactionId(transactionId).releaseThis();
                if (sharedTransactions.isEmpty()) {
                    cond.signalAll();
                }
            } else if (transactionId.equals(exclusiveTransaction)) {
                getWaitNodeByTransactionId(transactionId).releaseThis();
                exclusiveTransaction = null;
                cond.signalAll();
            }
            lock.unlock();
        }
    }

    private final HashMap<PageId, LockItem> lockMap = new HashMap<>();
    private final Lock mapLock = new ReentrantLock();

    public LockManager() {

    }

    public void aquireSharedLock(TransactionId transactionId, PageId pageId) throws TransactionAbortedException {
        mapLock.lock();
        LockItem lockItem = lockMap.get(pageId);
        if (lockItem == null) {
            lockItem = new LockItem();
            lockMap.put(pageId, lockItem);
        }
        mapLock.unlock();
        lockItem.sharedLock(transactionId);
    }

    public void aquireExclusiveLock(TransactionId transactionId, PageId pageId) throws TransactionAbortedException {
        mapLock.lock();
        LockItem lockItem = lockMap.get(pageId);
        if (lockItem == null) {
            lockItem = new LockItem();
            lockMap.put(pageId, lockItem);
        }
        mapLock.unlock();
        lockItem.exclusiveLock(transactionId);
    }

    public void releaseSharedLock(TransactionId transactionId, PageId pageId) throws DbException {
        mapLock.lock();
        LockItem lockItem = lockMap.get(pageId);
        if (lockItem == null) {
            mapLock.unlock();
            throw new DbException("releasing shared lock without aquiring");
        }
        mapLock.unlock();
        lockItem.releaseShared(transactionId);
    }

    public void releaseExclusiveLock(TransactionId transactionId, PageId pageId) throws DbException {
        mapLock.lock();
        LockItem lockItem = lockMap.get(pageId);
        if (lockItem == null) {
            mapLock.unlock();
            throw new DbException("releasing exclusive lock without aquiring");
        }
        mapLock.unlock();
        lockItem.releaseExclusive(transactionId);
    }

    public boolean isLocked(TransactionId transactionId, PageId pageId) {
        mapLock.lock();
        LockItem lockItem = lockMap.get(pageId);
        mapLock.unlock();
        if (lockItem == null) {
            return false;
        }
        return lockItem.isLocked(transactionId);
    }

    public void tryReleaseLock(TransactionId transactionId, PageId pageId) throws DbException {
        mapLock.lock();
        LockItem lockItem = lockMap.get(pageId);
        if (lockItem == null) {
            mapLock.unlock();
            throw new DbException("releasing exclusive lock without aquiring");
        }
        mapLock.unlock();
        lockItem.tryReleaseLock(transactionId);
    }

    public void releaseAllLocks(TransactionId transactionId) {
        mapLock.lock();
        for (var lockItem : lockMap.values()) {
            lockItem.tryReleaseLock(transactionId);
        }
        mapLock.unlock();
    }

    public class WaitGroupNode {
        private final HashSet<TransactionId> nexts = new HashSet<>();
        private final HashSet<TransactionId> prevs = new HashSet<>();
        private final TransactionId myId;
        private final Lock lock = new ReentrantLock();

        public WaitGroupNode(TransactionId transactionId) {
            myId = transactionId;
        }

        public void addWait(TransactionId transactionId) {
            lock.lock();
            var next = getWaitNodeByTransactionId(transactionId);
            this.nexts.add(transactionId);
            next.prevs.add(myId);
            lock.unlock();
        }

        public void addWait(Collection<TransactionId> tids) {
            for (var tid : tids) {
                addWait(tid);
            }
        }

        public void releaseThis() {
            lock.lock();
            for (var next : nexts) {
                var nextNode = getWaitNodeByTransactionId(next);
                nextNode.prevs.remove(myId);
            }
            for (var prev : prevs) {
                var prevNode = getWaitNodeByTransactionId(prev);
                prevNode.nexts.remove(myId);
            }
            nexts.clear();
            prevs.clear();
            lock.unlock();
        }

        private boolean checkCycleFromThis(HashSet<TransactionId> path, HashSet<TransactionId> checkedSet) {
            if (path.contains(this.myId)) {
                return true;
            }
            if (checkedSet.contains(myId)) {
                return false;
            }
            path.add(myId);
            for (var next : nexts) {
                if (checkedSet.contains(next)) {
                    continue;
                }
                if (getWaitNodeByTransactionId(next).checkCycleFromThis(path, checkedSet)) {
                    return true;
                }
            }
            path.remove(myId);
            checkedSet.add(myId);
            return false;
        }

        public boolean checkCycleFromThis() {
            lock.lock();
            boolean ret = checkCycleFromThis(new HashSet<>(), new HashSet<>());
            lock.unlock();
            return ret;
        }
    }

    private final HashMap<TransactionId, WaitGroupNode> waitNodeMap = new HashMap<>();

    public WaitGroupNode getWaitNodeByTransactionId(TransactionId transactionId) {
        WaitGroupNode node = waitNodeMap.get(transactionId);
        if (node == null) {
            node = new WaitGroupNode(transactionId);
            waitNodeMap.put(transactionId, node);
        }
        return node;
    }

}
