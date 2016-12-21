package simpledb;

import java.util.HashMap;
import java.util.ArrayList;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbFieldIndex;
    private Type gbFieldType;
    private int aggregateFieldIndex;
    private Op op;
    private HashMap<Field, Integer> count;
    
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) throws IllegalArgumentException {
        // some code goes here
    	gbFieldIndex = gbfield;
    	gbFieldType = gbfieldtype;
    	aggregateFieldIndex = afield;
    	op = what;
    	if (op != Op.COUNT) 
    		throw new IllegalArgumentException();
    	count = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	Field tupleGroupByField = (gbFieldIndex == Aggregator.NO_GROUPING) ? null : tup.getField(gbFieldIndex);
    	if (!count.containsKey(tupleGroupByField)) 
    		count.put(tupleGroupByField, 0);
    	int currentCount = count.get(tupleGroupByField);
    	count.put(tupleGroupByField, currentCount+1);
    }
    
    private TupleDesc createGroupByTupleDesc() {
    	String[] names;
    	Type[] types;
    	if (gbFieldIndex == Aggregator.NO_GROUPING) {
    		names = new String[] {"aggregateValue"};
    		types = new Type[] {Type.INT_TYPE};
    	} else {
    		names = new String[] {"groupValue", "aggregateValue"};
    		types = new Type[] {gbFieldType, Type.INT_TYPE};
    	}
    	return new TupleDesc(types, names);
    }
    
    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
    	ArrayList<Tuple> tuples = new ArrayList<Tuple>();
    	TupleDesc tupleDesc = createGroupByTupleDesc();
    	Tuple addMe;
    	for (Field group : count.keySet())
    	{
    		int aggregateVal = count.get(group);
    		addMe = new Tuple(tupleDesc);
    		if (gbFieldIndex == Aggregator.NO_GROUPING){
    			addMe.setField(0, new IntField(aggregateVal));
    		}
    		else {
        		addMe.setField(0, group);
        		addMe.setField(1, new IntField(aggregateVal));    			
    		}
    		tuples.add(addMe);
    	}
    	return new TupleIterator(tupleDesc, tuples);
    }

}
