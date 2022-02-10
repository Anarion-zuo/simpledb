package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.LockManager;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private final int numPages;
    private final LockManager lockManager = new LockManager();

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    private final HashMap<PageId, Page> allocated = new HashMap<>();
    private final LinkedList<PageId> pageAccessSeq = new LinkedList<>();

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        // lab 1 version
        if (perm.equals(Permissions.READ_ONLY)) {
            lockManager.aquireSharedLock(tid, pid);
        } else if (perm.equals(Permissions.READ_WRITE)) {
            lockManager.aquireExclusiveLock(tid, pid);
        } else {
            throw new DbException("unknown permission");
        }
        // no locks needed here
        // neccessary locks already attained
        return getAvailablePage(tid, pid);
    }

    private Page getAvailablePage(TransactionId tid, PageId pid) {
        // lab 1 version
        Page page = allocated.get(pid);
        if (page != null) {
            // move page to seq front
            pageAccessSeq.remove(pid);
            pageAccessSeq.add(pid);
            return page;
        }
        // must allocate
        DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        assert allocated.size() <= numPages;
        if (allocated.size() == numPages) {
            // must evict
            try {
                evictPage(tid, decideEvict());
            } catch (DbException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            // now allocated set is one page less than before
        }

        page = dbFile.readPage(pid);
        allocated.put(pid, page);
        pageAccessSeq.add(pid);
        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        try {
            lockManager.tryReleaseLock(tid, pid);
        } catch (DbException e) {
            e.printStackTrace();
        }
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.isLocked(tid, p);
    }

    private void abortDirtyPage(TransactionId tid, PageId pageId) {
        Page page = allocated.get(pageId);
        TransactionId dirtyId = page.isDirty();
        if (tid.equals(dirtyId)) {
            //allocated.remove(pageId);
            // should keep the in-memory version updated
            // heap page version
            assert page instanceof HeapPage;
            HeapPage heapPage = (HeapPage) page;
            HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(pageId.getTableId());
            try {
                heapPage.loadHeapData(heapFile.readPageData(pageId));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void abortPages(TransactionId tid) {
        for (var pageId : evictableSeq) {
            abortDirtyPage(tid, pageId);
        }
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        // TODO locks
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        var pages = file.insertTuple(tid, t);
        for (var page : pages) {
            page.markDirty(true, tid);
            // BufferPoolWriteTest changed to save this operation
            //file.writePage(page);
            //System.out.println("page first byte: " + page.getPageData()[0] + " page first tuple used " + ((HeapPage)page).isSlotUsed(0));
        }
        // TODO add versions
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        // TODO locks
        DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        var pages = file.deleteTuple(tid, t);
        for (var page : pages) {
            page.markDirty(true, tid);
        }
        // TODO add versions
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Flushes a certain page to disk
     * @param evictId an ID indicating the page to flush
     * @param tid transaction ID of the getPage operation
     */
    private synchronized void flushPage(PageId evictId, TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1
        DbFile evictFile = Database.getCatalog().getDatabaseFile(evictId.getTableId());
        Page evictPage = allocated.get(evictId);
        if (evictPage.isDirty() != null) {
            evictFile.writePage(evictPage);
            evictPage.markDirty(false, tid);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (var pageId : evictableSeq) {
            flushPage(pageId, tid);
        }
    }

    private PageId decideEvict(TransactionId transactionId) throws DbException {
        PageId ret = null;
        // least recently used first
        // NO STEAL
        for (PageId pageId : evictableSeq) {
            Page page = allocated.get(pageId);
            if (page.isDirty() == null) {
                ret = pageId;
                break;
            }
        }
        if (ret == null) {
            throw new DbException("all pages are dirty, cannot evict...");
        }
        evictableSeq.remove(ret);
        return ret;
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage(TransactionId tid, PageId evictId) throws DbException, IOException {
        // some code goes here
        // not necessary for lab1
        assert evictId != null;
        // lab2 naive dirty implementation
        flushPage(evictId, tid);
        allocated.remove(evictId);
    }

}
