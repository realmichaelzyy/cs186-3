package simpledb;

import java.util.HashMap;
import java.util.Iterator;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

	//	private HashMap<double[], Integer> histogram = new HashMap<double[], Integer>();
	//	private HashMap<Integer, double[]> keys = new HashMap<Integer, double[]>();
	private HashMap<Integer, Integer> histogram = new HashMap<Integer, Integer>();
	private int buckets;
	private int min;
	private int max;
	private int ntups;
	private double bucketSize;
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
		this.buckets = buckets;
		this.min = min;
		this.max = max;
		this.ntups = 0;

		this.bucketSize = Math.abs(((double)(max - min)) / (double)buckets);
		int counter = (int) Math.floor(min / this.bucketSize);
		for(double i=min; i<max ; i=i+this.bucketSize) {
			this.histogram.put(counter,0);
			counter++;
		}
	}

	public int findKey(int v){
		int quotient;
		if((double)v > ((double)this.max - this.bucketSize)){
			quotient = (int)Math.floor(v / this.bucketSize) - 1;
		}
		else{
			quotient = (int)Math.floor(v / this.bucketSize);
		}
		return quotient;
	}

	/**
	 * Add a value to the set of values that you are keeping a histogram of.
	 * @param v Value to add to the histogram
	 */
	public void addValue(int v) {
		// some code goes here
		int correspondingKey;
		try {
			if (v <= this.max || v >= this.min) {
				correspondingKey = findKey(v);
//				if (this.histogram.get(correspondingKey) == null){
//					System.out.println(correspondingKey + "is not a key in our histogram");
//					System.out.println("trying to find value:" + v);
//					System.out.println("there are this many buckets: "+ this.buckets);
//					System.out.println("the max is:" + this.max);
//					System.out.println("the min is:" + this.min);
//					System.out.println("the bucket size is:" + this.bucketSize);
//					System.out.println("here are the keys:");
//					Iterator<Integer>histogramKeyIterator = this.histogram.keySet().iterator();
//					while(histogramKeyIterator.hasNext()) {
//						System.out.println(histogramKeyIterator.next());
//					}
//					System.out.println("end of keys");
//				}
				this.histogram.put(correspondingKey, this.histogram.get(correspondingKey)+1);
				ntups++;
			}
			else {
				System.out.println("adding a value out of range");
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
		double toReturn = 0.0;
		int correspondingKey;
		double bf = 0.0;
		int vals = 0;
		double bPart = 0;

		if(v < this.min) {
			correspondingKey = Integer.MIN_VALUE;
		}
		else if(v > this.max) {
			correspondingKey = Integer.MAX_VALUE;
		}
		else {
			correspondingKey = findKey(v);
			bf = histogram.get(correspondingKey);
		}
		if (op.equals(Predicate.Op.GREATER_THAN) || op.equals(Predicate.Op.GREATER_THAN_OR_EQ) || op.equals(Predicate.Op.LESS_THAN_OR_EQ)
				|| op.equals(Predicate.Op.LESS_THAN)) {
			bPart = ((correspondingKey+1)*this.bucketSize - v)/this.bucketSize;
			Iterator<Integer>histogramKeyIterator = this.histogram.keySet().iterator();
			int currKey; 
			while(histogramKeyIterator.hasNext()) {
				currKey = histogramKeyIterator.next();
				if (correspondingKey < currKey) {
					vals += this.histogram.get(currKey);
				}
			}
			toReturn += (vals + bPart * bf)/this.ntups;	
		}
		if(op.equals(Predicate.Op.LESS_THAN) || op.equals(Predicate.Op.LESS_THAN_OR_EQ)) {
			toReturn = 1 - toReturn;
		}
		if (op.equals(Predicate.Op.EQUALS) || op.equals(Predicate.Op.GREATER_THAN_OR_EQ)|| op.equals(Predicate.Op.LESS_THAN_OR_EQ)) {
			toReturn += bf/this.ntups;
		}
		if (op.equals(Predicate.Op.GREATER_THAN_OR_EQ)|| op.equals(Predicate.Op.LESS_THAN_OR_EQ)) {
			toReturn -= (bPart* bf)/this.ntups;
		}
		if (op.equals(Predicate.Op.NOT_EQUALS)) {
			toReturn = 1-bf/this.ntups;
		}
		return toReturn;

	}

	/**
	 * @return
	 *     the average selectivity of this histogram.
	 *     
	 *     This is not an indispensable method to implement the basic
	 *     join optimization. It may be needed if you want to
	 *     implement a more efficient optimization
	 * */
	public double avgSelectivity()
	{
		// some code goes here
		return this.ntups/2;
	}

	/**
	 * @return A string describing this histogram, for debugging purposes
	 */
	public String toString() {
		// some code goes here
		return null;
	}

	public int getNTups() {
		return this.ntups;
	}
}
