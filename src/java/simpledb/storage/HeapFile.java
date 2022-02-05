package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * Keep this in mind !!! : One `HeapFile` object for each table.
 * `getId` returns the id of this file, aka this table.
 *
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        // done as suggested
        return file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        if (!(pid instanceof HeapPageId)) {
            return null;
        }
        HeapPage page = null;
        try {
            page = new HeapPage((HeapPageId) pid, new byte[BufferPool.getPageSize()]);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            RandomAccessFile reader = new RandomAccessFile(file, "r");
            reader.seek((long) pid.getPageNumber() * BufferPool.getPageSize());
            byte[] data = new byte[BufferPool.getPageSize()];
            reader.read(data, 0, BufferPool.getPageSize());
            page.loadHeapData(data);
            reader.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        if (!(page instanceof HeapPage)) {
            // deal with only heap page yet
            System.err.println("can only writing heap pages");
            return;
        }
        if (page.getId().getTableId() != getId()) {
            System.err.println("wrting a page belonging to another table");
        }
        // use OutputStream cannot get correct behavior
        // for no particular reason
        RandomAccessFile writer = new RandomAccessFile(file, "rw");
        writer.seek((long) BufferPool.getPageSize() * page.getId().getPageNumber());
        writer.write(page.getPageData());
        writer.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (file.length() / BufferPool.getPageSize());
    }

    /**
     * If there is no page available with an empty slot, it must allocate.
     * By allocating, this function writes null data to the offset of the page on disk.
     * @return The first page's index with an empty slot.
     */
    private Page fetchAvailablePage(TransactionId tid) throws IOException, TransactionAbortedException, DbException {
        // find available if possible
        for (int i = 0; i < numPages(); ++i) {
            PageId pid = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() != 0) {
                return page;
            }
        }
        // no available found
        extendFile(numPages());
        return Database.getBufferPool().getPage(tid, new HeapPageId(getId(), numPages() - 1), Permissions.READ_WRITE);
    }

    /**
     * Write null data to the offset of the page on disk.
     * @param pageIndex index of the page.
     */
    private void extendFile(int pageIndex) throws IOException {
        FileOutputStream outputStream = new FileOutputStream(file);
        outputStream.getChannel().position((long) pageIndex * BufferPool.getPageSize());
        byte[] data = HeapPage.createEmptyPageData();
        outputStream.write(data);
        outputStream.close();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        HeapPage page = (HeapPage) fetchAvailablePage(tid);
        RecordId recordId = new RecordId(page.getId(), 0);
        t.setRecordId(recordId);
        page.insertTuple(t);

        return new ArrayList<>(List.of(page));
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        PageId pageId = t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        page.deleteTuple(t);

        return new ArrayList<>(List.of(page));
    }

    public class HeapFileIterator implements DbFileIterator {

        static final int DEFAULT_POOL_SIZE = 100;

        TransactionId transactionId;
        HeapPageId curIndex;
        Iterator<Tuple> tupleIterator;

        boolean opened = false;   // lab1 naive impl

        HeapFileIterator(TransactionId tid) {
            transactionId = tid;
            curIndex = new HeapPageId(getId(), 0);
            tupleIterator = null;
        }

        /**
         * Move to the next available slot from an invalid slot.
         * The valid condition(s) on calling this function are:
         * - this iterator is pointing to an invalid slot
         * - this iterator is null
         */
        private void moveToNext() throws TransactionAbortedException, DbException {
            while (curIndex.getPageNumber() < numPages()) {
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(transactionId, curIndex, Permissions.READ_WRITE);
                tupleIterator = page.iterator();
                if (tupleIterator.hasNext()) {
                    return;
                }
                curIndex = new HeapPageId(curIndex.getTableId(), 1 + curIndex.getPageNumber());
            }
            // no next if code reaches here
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            if (opened) {
                throw new DbException("same iterator opened twice");
            }
            opened = true;
            moveToNext();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            /*
            if (curIndex.getPageNumber() >= heapFile.numPages()) {
                return false;
            }
            // move next would never stop until finding a used slot
            // stopping before the last page means I am on a used slot
            if (curIndex.getPageNumber() < heapFile.numPages() - 1) {
                return true;
            }
            // now only consider when curIndex is the last one.
            // check if I am a valid index
             */
            /**
             * We do not need what's above.
             * MoveNext process would never stop until a valid tuple iterator is created.
             * next() would certainly call moveNext at the end of its execution.
             * The iterator is ensured to be at the most probable valid place.
             * Or the iterator is not valid, meaning the iteration must end.
             */
            if (!opened) {
                //throw new DbException("HeapFileIterator not yet opened");
                return false;
            }
            return tupleIterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!opened) {
                throw new NoSuchElementException("accessing iterator not yet opened");
            }
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Tuple ret = tupleIterator.next();
            if (!tupleIterator.hasNext()) {
                curIndex = new HeapPageId(curIndex.getTableId(), 1 + curIndex.getPageNumber());
                // If tupleIterator.hasNext returns false,
                // and curIndex is currently at the last page,
                // moveToNext here would not change the value of any of the attributes,
                // leaving a hasNext call returning a correct result.
                moveToNext();
            }
            return ret;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            curIndex = new HeapPageId(curIndex.getTableId(), 0);
            tupleIterator = null;
            moveToNext();
        }

        @Override
        public void close() {
            opened = false;
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

}

