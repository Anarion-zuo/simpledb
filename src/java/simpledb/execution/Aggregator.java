package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.Tuple;
import simpledb.storage.TupleIterator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * The common interface for any class that can compute an aggregate over a
 * list of Tuples.
 * Changed to class from interface, for better code utility.
 */
public abstract class Aggregator implements Serializable {
    public static final int NO_GROUPING = -1;

    protected final int groupByField;
    protected final Type groupByFieldType;
    protected final int aggregateField;
    protected final Op operation;

    // simoutaneously implement 2 scenario
    // with and without group by
    // use groupMap with groupby
    protected final HashMap<Field, ArrayList<Tuple>> groupMap = new HashMap<>();
    // use nogroupList without groupby
    protected final ArrayList<Tuple> nogroupList = new ArrayList<>();

    /**
     * SUM_COUNT and SC_AVG will
     * only be used in lab7, you are not required
     * to implement them until then.
     * */
    public enum Op implements Serializable {
        MIN, MAX, SUM, AVG, COUNT,
        /**
         * SUM_COUNT: compute sum and count simultaneously, will be
         * needed to compute distributed avg in lab7.
         * */
        SUM_COUNT,
        /**
         * SC_AVG: compute the avg of a set of SUM_COUNT tuples,
         * will be used to compute distributed avg in lab7.
         * */
        SC_AVG;

        /**
         * Interface to access operations by a string containing an integer
         * index for command-line convenience.
         *
         * @param s a string containing a valid integer Op index
         */
        public static Op getOp(String s) {
            return getOp(Integer.parseInt(s));
        }

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         *
         * @param i a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }
        
        public String toString()
        {
        	if (this==MIN)
        		return "min";
        	if (this==MAX)
        		return "max";
        	if (this==SUM)
        		return "sum";
        	if (this==SUM_COUNT)
    			return "sum_count";
        	if (this==AVG)
        		return "avg";
        	if (this==COUNT)
        		return "count";
        	if (this==SC_AVG)
    			return "sc_avg";
        	throw new IllegalStateException("impossible to reach here");
        }
    }

    protected Aggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        groupByField = gbfield;
        groupByFieldType = gbfieldtype;
        aggregateField = afield;
        operation = what;
    }

    protected boolean hasNoGroupBy() {
        return groupByField == NO_GROUPING;
    }

    /**
     * Merge a new tuple into the aggregate for a distinct group value;
     * creates a new group aggregate result if the group value has not yet
     * been encountered.
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (hasNoGroupBy()) {
            // use nogroupList without groupby
            nogroupList.add(tup);
            return;
        }
        var field = getTupleGroupbyField(tup);
        var tupleList = groupMap.get(field);
        if (tupleList == null) {
            tupleList = new ArrayList<>();
            groupMap.put(field, tupleList);
        }
        tupleList.add(tup);
    }

    /**
     * Create a OpIterator over group aggregate results.
     * @see TupleIterator for a possible helper
     */
    abstract OpIterator iterator();

    protected Field getTupleGroupbyField(Tuple tuple) {
        return tuple.getField(groupByField);
    }
    
}
