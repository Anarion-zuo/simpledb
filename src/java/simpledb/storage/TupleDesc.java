package simpledb.storage;

import simpledb.common.DbException;
import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return items.iterator();
    }

    private static final long serialVersionUID = 1L;

    private final ArrayList<TDItem> items;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        /**
         * There should be some exception thrown here
         * for null argument
         * but the constructor's def does not allow it
         * so that's that
         */
        items = new ArrayList<>();
        for (int i = 0; i < typeAr.length; ++i) {
            if (fieldAr == null) {
                items.add(new TDItem(typeAr[i], null));
            } else {
                items.add(new TDItem(typeAr[i], fieldAr[i]));
            }
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        /**
         * There should be some exception thrown here
         * for null argument
         * but the constructor's def does not allow it
         * so that's that
         */
        items = new ArrayList<>();
        for (int i = 0; i < typeAr.length; ++i) {
            items.add(new TDItem(typeAr[i], null));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return items.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        return items.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        return items.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        for (int i = 0; i < items.size(); ++i) {
            if (items.get(i).fieldName == null) {
                if (name == null) {
                    return i;
                }
            }
            if (items.get(i).fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int ret = 0;
        for (TDItem item : items) {
            ret += item.fieldType.getLen();
        }
        return ret;
    }

    /**
     * Constructor. Create a TupleDesc object with a initialized TDItem array.
     * Helper function not for public calling.
     * @param items
     */
    private TupleDesc(ArrayList<TDItem> items) {
        this.items = items;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        ArrayList<TDItem> items = new ArrayList<>();
        items.addAll(td1.items);
        items.addAll(td2.items);
        return new TupleDesc(items);
    }

    /**
     * Add prefix to each field name.
     * @return the new desc
     */
    public TupleDesc prefixDesc(String prefix) {
        ArrayList<TDItem> newItems = new ArrayList<>();
        for (TDItem item : items) {
            newItems.add(new TDItem(item.fieldType, prefix + item.fieldName));
        }
        return new TupleDesc(newItems);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if (o == null) {
            return false;
        }
        if (!(o instanceof TupleDesc)) {
            return false;
        }
        TupleDesc rhs = (TupleDesc)o;
        if (rhs.numFields() != this.numFields()) {
            return false;
        }
        for (int i = 0; i < this.numFields(); ++i) {
            if (!this.items.get(i).fieldType.equals(rhs.items.get(i).fieldType)) {
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented hashCode");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append('(');
        for (int i = 0; i < items.size(); ++i) {
            TDItem item = items.get(i);
            stringBuilder.append(String.format("%s[%d](%s[%d])", item.fieldType.toString(), i, item.fieldName, i));
            if (i < items.size() - 1) {
                stringBuilder.append(',');
            }
        }
        return stringBuilder.toString();
    }
}
