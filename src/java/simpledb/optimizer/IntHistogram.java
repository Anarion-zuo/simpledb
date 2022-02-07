package simpledb.optimizer;

import simpledb.execution.Predicate;
import simpledb.storage.Field;
import simpledb.storage.IntField;

import java.util.Arrays;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram implements Histogram {

    private int[] buckets;
    private int minVal, maxVal;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        /**
         * We must check whether the input range has more numbers than #buckets.
         * Or one bucket contains less than one number.
         */
        if (max - min + 1 < buckets) {
            buckets = max - min + 1;
        }
        this.buckets = new int[buckets];
        Arrays.fill(this.buckets, 0);

        // [min, max)
        // +1 to have correct behavoir when input is max
        this.maxVal = max + 1;
        this.minVal = min;
    }

    private int getBucketIndexByVal(int val) {
        assert val <= maxVal && val >= minVal;
        int ret = (val - minVal) * buckets.length / (maxVal - minVal);
        assert ret <= buckets.length && ret >= 0;
        return ret;
    }

    private double valsPerBucket() {
        return (maxVal - minVal) / (double)buckets.length;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        buckets[getBucketIndexByVal(v)]++;
    }

    private double estimateEquals(int val) {
        if (val < minVal || val >= maxVal) {
            return 0;
        }
        int bucketCount = buckets[getBucketIndexByVal(val)];
        return bucketCount / valsPerBucket();
    }

    public int getIntervalMin(int index) {
        return (maxVal - minVal) * index / buckets.length + minVal;
    }

    // open bracket
    public int getIntervalMax(int index) {
        return getIntervalMin(index + 1);
    }

    private int totalCount() {
        int ret = 0;
        for (int bucket : buckets) {
            ret += bucket;
        }
        return ret;
    }

    // [left, right)
    private double estimateInterval(int left, int right) {
        if (left >= right || right <= minVal || left >= maxVal) {
            return 0;
        }
        if (left < minVal) {
            left = minVal;
        }
        if (right > maxVal) {
            right = maxVal;
        }
        int leftBucketIndex = getBucketIndexByVal(left);
        int rightBucketIndex = getBucketIndexByVal(right);
        int midCount = 0;
        for (int i = leftBucketIndex + 1; i < rightBucketIndex; ++i) {
            midCount += buckets[i];
        }
        double leftCount = (getIntervalMax(leftBucketIndex) - left) * buckets[leftBucketIndex] / valsPerBucket();
        double rightCount = (right - getIntervalMin(rightBucketIndex)) * buckets[leftBucketIndex] / valsPerBucket();
        return leftCount + rightCount + midCount;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        return switch (op) {
            case EQUALS -> estimateEquals(v);
            case GREATER_THAN -> estimateInterval(v, maxVal) - estimateEquals(v);
            case GREATER_THAN_OR_EQ -> estimateInterval(v, maxVal);
            case LESS_THAN -> estimateInterval(minVal, v);
            case LESS_THAN_OR_EQ -> estimateInterval(minVal, v) + estimateEquals(v);
            case NOT_EQUALS -> totalCount() - estimateEquals(v);
            default -> -1.0;
        } / totalCount();
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity() {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < buckets.length; ++i) {
            builder.append('[');
            builder.append(getIntervalMin(i));
            builder.append(',');
            builder.append(getIntervalMax(i));
            builder.append("):");
            builder.append(buckets[i]);
            builder.append(' ');
        }
        return builder.toString();
    }

    @Override
    public void addValue(Field field) {
        assert field instanceof IntField;
        IntField intField = (IntField) field;
        addValue(intField.getValue());
    }

    @Override
    public double estimateSelectivity(Predicate.Op op, Field field) {
        assert field instanceof IntField;
        IntField intField = (IntField) field;
        return estimateSelectivity(op, intField.getValue());
    }
}
