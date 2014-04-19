package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

	private static final long serialVersionUID = 1L;
	private TransactionId tid;
	private DbIterator child;
	private int tableId;
	private DbIterator aggregateIterator;
	private boolean calledFetchNext;
	private TupleDesc tupleDesc;

	/**
	 * Constructor.
	 * 
	 * @param t
	 *            The transaction running the insert.
	 * @param child
	 *            The child operator from which to read tuples to be inserted.
	 * @param tableid
	 *            The table in which to insert tuples.
	 * @throws DbException
	 *             if TupleDesc of child differs from table into which we are to
	 *             insert.
	 */
	public Insert(TransactionId t,DbIterator child, int tableid)
			throws DbException {
		// some code goes here
		//		if(!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableid))) {
		//			throw new DbException("Child TupleDesc does not match table");
		//		}
		this.tid = t;
		this.child = child;
		this.tableId = tableid;
	}

	public TupleDesc getTupleDesc() {
		// some code goes here
		if(this.tupleDesc == null) {
			this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
		}
		return this.tupleDesc;
	}

	public void open() throws DbException, TransactionAbortedException {
		// some code goes here
		this.child.open();
		this.calledFetchNext = false;
		super.open();
	}

	public void close() {
		// some code goes here
		this.child.close();
		super.close();
	}

	public void rewind() throws DbException, TransactionAbortedException {
		// some code goes here
		child.rewind();
	}

	/**
	 * Inserts tuples read from child into the tableid specified by the
	 * constructor. It returns a one field tuple containing the number of
	 * inserted records. Inserts should be passed through BufferPool. An
	 * instances of BufferPool is available via Database.getBufferPool(). Note
	 * that insert DOES NOT need check to see if a particular tuple is a
	 * duplicate before inserting it.
	 * 
	 * @return A 1-field tuple containing the number of inserted records, or
	 *         null if called more than once.
	 * @see Database#getBufferPool
	 * @see BufferPool#insertTuple
	 */
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		// some code goes here
		if(this.calledFetchNext) {
			return null;
		}
		int count = 0;
		while(this.child.hasNext()) {
			try {
				Database.getBufferPool().insertTuple(this.tid, this.tableId, this.child.next());
			} catch (NoSuchElementException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			count++;
		}
		Tuple toReturn = new Tuple(getTupleDesc());
		toReturn.setField(0, new IntField(count));
		this.calledFetchNext = true;
		return toReturn;
	}

	@Override
	public DbIterator[] getChildren() {
		// some code goes here
		return null;
	}

	@Override
	public void setChildren(DbIterator[] children) {
		// some code goes here
	}
}
