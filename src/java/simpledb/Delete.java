package simpledb;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
	private TransactionId tid;
	private DbIterator child;
	private TupleDesc tupleDesc;
	private boolean calledFetchNext;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
    	this.tid = t;
    	this.child = child;
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
    	this.calledFetchNext = false;
    	child.open();
    	super.open();
    }

    public void close() {
        // some code goes here
    	child.close();
    	super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(this.calledFetchNext) {
        	return null;
        }
        int count = 0;
        while(this.child.hasNext()) {
        	Database.getBufferPool().deleteTuple(this.tid, this.child.next());
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
