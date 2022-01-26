package simpledb.storage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    private TupleDesc tupleDesc;
    private final Field[] fieldList;
    private RecordId recordId = null;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        tupleDesc = td;
        fieldList = new Field[td.numFields()];
        Arrays.fill(fieldList, null);
    }

    /**
     * Called only by static utility methods.
     * @param td
     * @param fieldList
     */
    private Tuple(TupleDesc td, Field[] fieldList) {
        this.tupleDesc = td;
        this.fieldList = fieldList;
    }

    public static Tuple merge(Tuple t1, Tuple t2) {
        Field[] fieldList = new Field[t1.fieldList.length + t2.fieldList.length];
        for (int i = 0; i < t1.fieldList.length; ++i) {
            fieldList[i] = t1.fieldList[i];
        }
        for (int i = 0; i < t2.fieldList.length; ++i) {
            fieldList[i + t1.fieldList.length] = t2.fieldList[i];
        }
        return new Tuple(TupleDesc.merge(t1.getTupleDesc(), t2.getTupleDesc()), fieldList);
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        fieldList[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        return fieldList[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        //throw new UnsupportedOperationException("Implement this");
        StringBuilder ret = new StringBuilder("");
        for (int i = 0; i < fieldList.length - 1; ++i) {
            ret.append(fieldList[i].toString());
            ret.append('\t');
        }
        ret.append(fieldList[fieldList.length - 1].toString());
        return ret.toString();
    }

    static class FieldIterator implements Iterator<Field> {
        int index;
        Field[] fields;

        FieldIterator(int index, Field[] fields) {
            this.index = index;
            this.fields = fields;
        }

        @Override
        public boolean hasNext() {
            return index + 1 < fields.length;
        }

        @Override
        public Field next() {
            return fields[index++];
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return new FieldIterator(0, fieldList);
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
        // TODO what is this???
        tupleDesc = td;
    }
}
