package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbFieldIndex;
    private Type gbFieldType;
    private int aggregateFieldIndex;
    private Op op;
    private HashMap<Field, Integer> aggregateData;
    private HashMap<Field, Integer> count;
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
    	gbFieldIndex = gbfield;
    	gbFieldType = gbfieldtype;
    	aggregateFieldIndex = afield;
    	op = what;
    	aggregateData = new HashMap<Field, Integer>();
    	count = new HashMap<Field, Integer>();
    }
    
    private int initializeData() {
    	switch (op) {
    		case MIN: return Integer.MAX_VALUE;
    		case MAX: return Integer.MIN_VALUE;
    		case SUM: case COUNT: case AVG: return 0;
    		default: return 0;
    	}
    }
    
    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	Field tupleGroupByField = (gbFieldIndex == Aggregator.NO_GROUPING) ? null : tup.getField(gbFieldIndex);
    	if (!aggregateData.containsKey(tupleGroupByField)) {
    		aggregateData.put(tupleGroupByField, initializeData());
    		count.put(tupleGroupByField, 0);
    	}
    	
    	int tupleValue = ((IntField) tup.getField(aggregateFieldIndex)).getValue();
    	int currentValue = aggregateData.get(tupleGroupByField);
    	int currentCount = count.get(tupleGroupByField);
    	int newValue = currentValue;
    	switch(op) {
    		case MIN:
    			newValue = (tupleValue > currentValue) ? currentValue : tupleValue;
    			break;
    		case MAX:
    			newValue = (tupleValue > currentValue) ? tupleValue : currentValue;
    			break;
    		case SUM: case AVG:
    			count.put(tupleGroupByField, currentCount+1);
    			newValue = tupleValue + currentValue;
    			break;
    		case COUNT:
    			newValue = currentValue + 1;
    			break;
    		default:
    			break;
    	}
    	aggregateData.put(tupleGroupByField, newValue);
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
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
    	ArrayList<Tuple> tuples = new ArrayList<Tuple>();
    	TupleDesc tupleDesc = createGroupByTupleDesc();
    	Tuple addMe;
    	for (Field group : aggregateData.keySet()) {
    		int aggregateValue;
    		if (op == Op.AVG)
    			aggregateValue = aggregateData.get(group) / count.get(group);
    		else
    			aggregateValue = aggregateData.get(group);
    		addMe = new Tuple(tupleDesc);
    		if (gbFieldIndex == Aggregator.NO_GROUPING)
    			addMe.setField(0, new IntField(aggregateValue));
    		else {
    			addMe.setField(0, group);
    			addMe.setField(1, new IntField(aggregateValue));
    		}
    		tuples.add(addMe);
    	}
    	return new TupleIterator(tupleDesc, tuples);
    }

}
