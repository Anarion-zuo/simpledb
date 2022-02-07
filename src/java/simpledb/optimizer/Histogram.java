package simpledb.optimizer;

import simpledb.execution.Predicate;
import simpledb.storage.Field;

public interface Histogram {
    public void addValue(Field field);
    public double estimateSelectivity(Predicate.Op op, Field field);
}
