package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator extends Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        super(gbfield, gbfieldtype, afield, what);
    }

    private int getTupleAggregateVal(Tuple tuple) {
        return ((IntField) tuple.getField(aggregateField)).getValue();
    }

    private class TupleComparator implements Comparator<Tuple> {
        @Override
        public int compare(Tuple x, Tuple y) {
            int left = getTupleAggregateVal(x);
            int right = getTupleAggregateVal(y);
            return Integer.compare(left, right);
        }
    }

    /**
     * Executes aggregate schemes.
     * @param tupleList List of tuples to be aggregated.
     * @return result of aggregation
     */
    private int doAggregate(ArrayList<Tuple> tupleList) {
        switch (operation) {
            case MIN:
                return getTupleAggregateVal(tupleList.stream().min(new TupleComparator()).get());
            case MAX:
                return getTupleAggregateVal(tupleList.stream().max(new TupleComparator()).get());
            case SUM:
                int ret = 0;
                for (var tuple : tupleList) {
                    ret += getTupleAggregateVal(tuple);
                }
                return ret;
            case COUNT:
                return tupleList.size();
            case AVG:
                ret = 0;
                for (var tuple : tupleList) {
                    ret += getTupleAggregateVal(tuple);
                }
                return ret / tupleList.size();
            default:
//                throw new ExecutionControl.NotImplementedException("aggregate operation not implemented");
        }
        return -1;
    }

    private class AggregatorOperator implements OpIterator {

        Iterator<Map.Entry<Field, ArrayList<Tuple>>> mapIterator;
        boolean noGroupAccessed = false;
        boolean opened = false;

        public AggregatorOperator() {
            initEntryIterator();
        }

        private void initEntryIterator() {
            mapIterator = groupMap.entrySet().iterator();
            noGroupAccessed = false;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            opened = true;
            initEntryIterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!this.opened)
                throw new IllegalStateException("Operator not yet open");
            if (hasNoGroupBy()) {
                return !noGroupAccessed;
            }
            return mapIterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!this.opened)
                throw new IllegalStateException("Operator not yet open");
            if (hasNoGroupBy()) {
                noGroupAccessed = true;
                return new Tuple(getTupleDesc(), new Field[] { new IntField(doAggregate(nogroupList))});
            } else {
                var entry = mapIterator.next();
                var groupVal = entry.getKey();
                var groupTuple = entry.getValue();
                return new Tuple(getTupleDesc(), new Field[] {
                        groupVal,
                        new IntField(doAggregate(groupTuple))
                });
            }
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if (!this.opened)
                throw new IllegalStateException("Operator not yet open");
            initEntryIterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            if (hasNoGroupBy()) {
                return new TupleDesc(new Type[] {Type.INT_TYPE});
            } else {
                return new TupleDesc(
                    new Type[] {groupByFieldType, Type.INT_TYPE}
                    // does not provide names for fields
                );
            }
        }

        @Override
        public void close() {
            if (!this.opened)
                throw new IllegalStateException("Operator not yet open");
            opened = false;
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new AggregatorOperator();
    }

}
