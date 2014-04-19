package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

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

	private int gbField;
	private Type gbFieldType;
	private int aField;
	private Op what;
	private HashMap<Field, Integer> aggregateVal;
	private HashMap<Field, Integer> aggregateCounter;

	public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		// some code goes here

		this.gbField = gbfield;
		this.gbFieldType = gbfieldtype;

		this.aField = afield;
		this.what = what;
		this.aggregateVal = new HashMap<Field, Integer>();
		this.aggregateCounter = new HashMap<Field, Integer>();
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
		int value;
		Field key;
		if (this.gbField == Aggregator.NO_GROUPING) {
			key = new IntField(-1);
		}
		else {
			key = tup.getField(this.gbField);
		}

		if(aggregateVal.get(key) == null) {
			if(this.what.toString().equals("min")) {
				value = Integer.MAX_VALUE;
			}
			else if(this.what.toString().equals("max")) {
				value = Integer.MIN_VALUE;
			}
			else {
				value = 0;
			}
		}
		else {
			value = aggregateVal.get(key);
		}

		if(this.what.toString().equals("min")) {
			aggregateVal.put(key, Math.min(value, ((IntField)tup.getField(this.aField)).getValue()));
		}
		else if(this.what.toString().equals("max")) {
			aggregateVal.put(key, Math.max(value, ((IntField)tup.getField(this.aField)).getValue()));
		}
		else if(this.what.toString().equals("sum") || this.what.toString().equals("avg")) {
			aggregateVal.put(key, value + ((IntField)tup.getField(this.aField)).getValue());
		}
		if(this.what.toString().equals("count") || this.what.toString().equals("avg")) {
			if(aggregateCounter.get(key) == null) {
				aggregateCounter.put(key, 1);
			}
			else {
				aggregateCounter.put(key, aggregateCounter.get(key) + 1);
			}
		}
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
		TupleDesc tupleDesc;
		Type [] descTypes;
		String [] descNames;
		if(this.gbField != Aggregator.NO_GROUPING) {
			if (this.gbFieldType == null){ descTypes = new Type[]{Type.INT_TYPE,Type.INT_TYPE};}
			else 
				descTypes = new Type[]{this.gbFieldType,Type.INT_TYPE};
			descNames = new String[]{"groupVal", "aggregateVal"};

		}
		else {
			descTypes = new Type[]{Type.INT_TYPE};
			descNames = new String[]{"aggregateVal"};			
		}
		tupleDesc = new TupleDesc(descTypes, descNames);

		ArrayList<Tuple> tuples = new ArrayList<Tuple>();
		Iterator<Field> keyIterator;
		if(this.what.toString().equals("count")) { keyIterator = this.aggregateCounter.keySet().iterator();}
		else { keyIterator = this.aggregateVal.keySet().iterator();}
		Field currentKey;
		Tuple toAdd;

		while(keyIterator.hasNext()) {
			currentKey = keyIterator.next();
			toAdd = new Tuple(tupleDesc);

			if(this.gbField != Aggregator.NO_GROUPING) {

				toAdd.setField(0, currentKey);

				if(this.what.toString().equals("count")) { 
					toAdd.setField(1, new IntField(this.aggregateCounter.get(currentKey)));						
				}
				else if(this.what.toString().equals("avg")) { 
					toAdd.setField(1, new IntField(this.aggregateVal.get(currentKey)/this.aggregateCounter.get(currentKey)));					
				}
				else {
					toAdd.setField(1, new IntField(this.aggregateVal.get(currentKey)));
				}
			}
			else {

				if(this.what.toString().equals("count")) { 
					toAdd.setField(0, new IntField(this.aggregateCounter.get(currentKey)));			
					System.out.println("No Group count ");		
				}
				else if(this.what.toString().equals("avg")) { 
					toAdd.setField(0, new IntField(this.aggregateVal.get(currentKey)/this.aggregateCounter.get(currentKey)));					
				}
				else {
					toAdd.setField(0, new IntField(this.aggregateVal.get(currentKey)));

				}
			}
			tuples.add(toAdd);
		}
		return new TupleIterator(tupleDesc, tuples);
	}
}
