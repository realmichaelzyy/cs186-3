package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

	private static final long serialVersionUID = 1L;
	private int gbField;
	private Type gbFieldType;
	private int aField;
	private Op what;
	private HashMap<Field, Integer> aggregateCounter;

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
		if(what != Op.COUNT) {
			throw new IllegalArgumentException();
		}
		this.gbField = gbfield;
		this.gbFieldType = gbfieldtype;
		this.aField = afield;
		this.what = what;
		this.aggregateCounter = new HashMap<Field, Integer>();
	}

	/**
	 * Merge a new tuple into the aggregate, grouping as indicated in the constructor
	 * @param tup the Tuple containing an aggregate field and a group-by field
	 */
	public void mergeTupleIntoGroup(Tuple tup) {
		// some code goes here
		Field key;
		if (this.gbFieldType == null) {
			key = new IntField(-1);
		}
		else {
			key = tup.getField(this.gbField);
		}

		if(aggregateCounter.get(key) == null) {
			aggregateCounter.put(key, 1);
		}
		else {
			aggregateCounter.put(key, aggregateCounter.get(key) + 1);
		}
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
		TupleDesc tupleDesc;
		Type [] descTypes;
		String [] descNames;

		if(this.gbFieldType != null) {
			descTypes = new Type[]{this.gbFieldType,Type.INT_TYPE};
			descNames = new String[]{"groupVal", "aggregateVal"};
		}
		else {
			descTypes = new Type[]{Type.INT_TYPE};
			descNames = new String[]{"aggregateVal"};		
		}
		tupleDesc = new TupleDesc(descTypes, descNames);

		ArrayList<Tuple> tuples = new ArrayList<Tuple>();
		Iterator<Field> keyIterator = this.aggregateCounter.keySet().iterator();
		Field currentKey;
		Tuple toAdd;
		while(keyIterator.hasNext()) {
			currentKey = keyIterator.next();
			toAdd = new Tuple(tupleDesc);
			if(this.gbFieldType != null) {
				toAdd.setField(0, currentKey);
				toAdd.setField(1, new IntField(this.aggregateCounter.get(currentKey)));					
			}
			else {
				toAdd.setField(0, new IntField(this.aggregateCounter.get(currentKey)));					
			}
			tuples.add(toAdd);
		}
		return new TupleIterator(tupleDesc, tuples);
	}
}
