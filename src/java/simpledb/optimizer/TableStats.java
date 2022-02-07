package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import java.lang.Math;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private final Histogram []histograms;
    private final int ioCostPerPage;
    private final DbFile dbFile;
    private final int tupleCount;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.ioCostPerPage = ioCostPerPage;
        this.dbFile = Database.getCatalog().getDatabaseFile(tableid);
        var it = dbFile.iterator(new TransactionId());
        try {
            it.open();
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }
        TupleDesc desc = dbFile.getTupleDesc();
        // find min max
        Field[][] minmax = findMinMaxOnAllFields(it, desc);
        Field[] mins = minmax[0];
        Field[] maxs = minmax[1];
        histograms = new Histogram[desc.numFields()];
        initHistograms(desc, mins, maxs);
        // add fields to histograms
        this.tupleCount = addFieldsToHistograms(it);
        it.close();
    }

    /**
     * Called only by constructor
     */
    private Field[][] findMinMaxOnAllFields(DbFileIterator iterator, TupleDesc desc) {
        Tuple first = null;
        try {
            iterator.rewind();
            first = iterator.next();
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }
        assert first != null;
        // TODO ignores cases where the file is empty
        Field[] minFields = new Field[desc.numFields()];
        Field[] maxFields = new Field[desc.numFields()];
        for (int i = 0; i < desc.numFields(); ++i) {
            minFields[i] = first.getField(i);
            maxFields[i] = first.getField(i);
        }
        try {
            while (iterator.hasNext()) {
                Tuple tuple = iterator.next();
                for (int i = 0; i < desc.numFields(); ++i) {
                    Field field = tuple.getField(i);
                    if (field.compare(Predicate.Op.LESS_THAN, minFields[i])) {
                        minFields[i] = field;
                    }
                    if (field.compare(Predicate.Op.GREATER_THAN, maxFields[i])) {
                        maxFields[i] = field;
                    }
                }
            }
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }
        return new Field[][] {minFields, maxFields};
    }

    /**
     * Called only by constructor
     */
    private void initHistograms(TupleDesc desc, Field[] mins, Field[] maxs) {
        for (int i = 0; i < histograms.length; ++i) {
            Type type = desc.getFieldType(i);
            if (type.equals(Type.INT_TYPE)) {
                histograms[i] = new IntHistogram(NUM_HIST_BINS, ((IntField)mins[i]).getValue(), ((IntField)maxs[i]).getValue());
            } else if (type.equals(Type.STRING_TYPE)) {
                // TODO better string histogram
                histograms[i] = new StringHistogram(NUM_HIST_BINS);
            } else {
                System.err.println("unknown type encountered when building histograms");
                System.exit(1);
            }
        }
    }

    /**
     * Called only by constructor
     * @return tuple count
     */
    private int addFieldsToHistograms(DbFileIterator iterator) {
        int tupCount = 0;
        try {
            iterator.rewind();
            while (iterator.hasNext()) {
                Tuple tuple = iterator.next();
                assert histograms.length == tuple.getTupleDesc().numFields();
                tupCount++;
                for (int i = 0; i < histograms.length; ++i) {
                    histograms[i].addValue(tuple.getField(i));
                }
            }
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }
        return tupCount;
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        // TODO only heap file for now
        assert dbFile instanceof HeapFile;
        return ioCostPerPage * ((HeapFile)dbFile).numPages();
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        assert dbFile instanceof HeapFile;
        HeapFile heapFile = (HeapFile) dbFile;
        return (int)Math.ceil(tupleCount * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here

        // should return possible #pages
        assert dbFile instanceof HeapFile;
        HeapFile heapFile = (HeapFile) dbFile;
        return histograms[field].estimateSelectivity(op, constant);
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return tupleCount;
    }

}
