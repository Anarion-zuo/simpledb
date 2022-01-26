package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator extends Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        super(gbfield, gbfieldtype, afield, what);
    }

    private String getTupleAggregateVal(Tuple tuple) {
        return ((StringField) tuple.getField(aggregateField)).getValue();
    }

    private class TupleComparator implements Comparator<Tuple> {
        @Override
        public int compare(Tuple x, Tuple y) {
            return getTupleAggregateVal(x).compareTo(getTupleAggregateVal(y));
        }
    }

    /**
     * Executes aggregate schemes.
     * @param tupleList List of tuples to be aggregated.
     * @return result of aggregation
     */
    protected int doAggregate(ArrayList<Tuple> tupleList) {
        switch (operation) {
            /*case MIN:
                return getTupleAggregateVal(tupleList.stream().min(new TupleComparator()).get());
            case MAX:
                return getTupleAggregateVal(tupleList.stream().max(new TupleComparator()).get());
            case SUM:
                int ret = 0;
                for (var tuple : tupleList) {
                    ret += getTupleAggregateVal(tuple);
                }
                return ret;*/
            case COUNT:
                return tupleList.size();
            /*case AVG:
                ret = 0;
                for (var tuple : tupleList) {
                    ret += getTupleAggregateVal(tuple);
                }
                return ret / tupleList.size();*/
            default:
//                throw new ExecutionControl.NotImplementedException("aggregate operation not implemented");
        }
        return -1;
    }

}
