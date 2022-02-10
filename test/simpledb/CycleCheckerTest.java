package simpledb;

import org.junit.Assert;
import org.junit.Test;
import simpledb.transaction.LockManager;
import simpledb.transaction.TransactionId;

import java.util.ArrayList;

public class CycleCheckerTest {

    private final LockManager lockManager = new LockManager();

    @Test public void pointToSelfTest() {
        TransactionId tid = new TransactionId();
        var node = lockManager.getWaitNodeByTransactionId(tid);
        node.addWait(tid);
        Assert.assertTrue(node.checkCycleFromThis());
        node.releaseThis();
        Assert.assertFalse(node.checkCycleFromThis());
    }

    @Test public void twoNodesTest() {
        TransactionId tid1 = new TransactionId();
        TransactionId tid2 = new TransactionId();
        var n1 = lockManager.getWaitNodeByTransactionId(tid1);
        var n2 = lockManager.getWaitNodeByTransactionId(tid2);
        n1.addWait(tid2);
        n2.addWait(tid1);
        Assert.assertTrue(n1.checkCycleFromThis());
        Assert.assertTrue(n2.checkCycleFromThis());
        n1.releaseThis();
        Assert.assertFalse(n1.checkCycleFromThis());
        Assert.assertFalse(n2.checkCycleFromThis());
    }

    @Test public void threeNodesTest() {
        new TransactionId();
        TransactionId tid1 = new TransactionId();
        TransactionId tid2 = new TransactionId();
        TransactionId tid3 = new TransactionId();
        var n1 = lockManager.getWaitNodeByTransactionId(tid1);
        var n2 = lockManager.getWaitNodeByTransactionId(tid2);
        var n3 = lockManager.getWaitNodeByTransactionId(tid3);
        n1.addWait(tid2);
        n2.addWait(tid3);
        n3.addWait(tid1);
        Assert.assertTrue(n1.checkCycleFromThis());
        Assert.assertTrue(n2.checkCycleFromThis());
        Assert.assertTrue(n3.checkCycleFromThis());
        n1.releaseThis();
        Assert.assertFalse(n1.checkCycleFromThis());
        Assert.assertFalse(n2.checkCycleFromThis());
        Assert.assertFalse(n3.checkCycleFromThis());
        n2.releaseThis();
        Assert.assertFalse(n1.checkCycleFromThis());
        Assert.assertFalse(n2.checkCycleFromThis());
        Assert.assertFalse(n3.checkCycleFromThis());
    }

    @Test public void manyNodesTest() {
        ArrayList<TransactionId> tids = new ArrayList<>();
        final int tidCount = 1000;
        TransactionId tid0 = new TransactionId();
        tids.add(tid0);
        var prev = tid0;
        for (int i = 1; i < tidCount; ++i) {
            TransactionId tid = new TransactionId();
            tids.add(tid);
            lockManager.getWaitNodeByTransactionId(tid).addWait(prev);
            prev = tid;
        }
        lockManager.getWaitNodeByTransactionId(tid0).addWait(prev);
        for (var tid : tids) {
            Assert.assertTrue(lockManager.getWaitNodeByTransactionId(tid).checkCycleFromThis());
        }
        lockManager.getWaitNodeByTransactionId(tids.get(tidCount / 3)).releaseThis();
        for (var tid : tids) {
            Assert.assertFalse(lockManager.getWaitNodeByTransactionId(tid).checkCycleFromThis());
        }
    }

    @Test public void twoCyclesTest() {
        TransactionId tid0 = new TransactionId();
        TransactionId tid1 = new TransactionId();
        TransactionId tid2 = new TransactionId();
        TransactionId tid3 = new TransactionId();
        TransactionId tid4 = new TransactionId();
        /**
         * 0 - 1
         *  \ /
         *   2
         *  / \
         * 3 - 4
         */
        lockManager.getWaitNodeByTransactionId(tid0).addWait(tid2);
        lockManager.getWaitNodeByTransactionId(tid2).addWait(tid1);
        lockManager.getWaitNodeByTransactionId(tid1).addWait(tid0);
        lockManager.getWaitNodeByTransactionId(tid3).addWait(tid2);
        lockManager.getWaitNodeByTransactionId(tid2).addWait(tid4);
        lockManager.getWaitNodeByTransactionId(tid4).addWait(tid3);
        Assert.assertTrue(lockManager.getWaitNodeByTransactionId(tid0).checkCycleFromThis());
        Assert.assertTrue(lockManager.getWaitNodeByTransactionId(tid1).checkCycleFromThis());
        Assert.assertTrue(lockManager.getWaitNodeByTransactionId(tid2).checkCycleFromThis());
        Assert.assertTrue(lockManager.getWaitNodeByTransactionId(tid3).checkCycleFromThis());
        Assert.assertTrue(lockManager.getWaitNodeByTransactionId(tid4).checkCycleFromThis());
        lockManager.getWaitNodeByTransactionId(tid1).releaseThis();
        Assert.assertFalse(lockManager.getWaitNodeByTransactionId(tid1).checkCycleFromThis());
        Assert.assertTrue(lockManager.getWaitNodeByTransactionId(tid0).checkCycleFromThis());
        Assert.assertTrue(lockManager.getWaitNodeByTransactionId(tid2).checkCycleFromThis());
        lockManager.getWaitNodeByTransactionId(tid0).releaseThis();
        Assert.assertFalse(lockManager.getWaitNodeByTransactionId(tid0).checkCycleFromThis());
        Assert.assertTrue(lockManager.getWaitNodeByTransactionId(tid2).checkCycleFromThis());
        lockManager.getWaitNodeByTransactionId(tid2).releaseThis();
        Assert.assertFalse(lockManager.getWaitNodeByTransactionId(tid2).checkCycleFromThis());
        Assert.assertFalse(lockManager.getWaitNodeByTransactionId(tid3).checkCycleFromThis());
        Assert.assertFalse(lockManager.getWaitNodeByTransactionId(tid4).checkCycleFromThis());
    }
}
