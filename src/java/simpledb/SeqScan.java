package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     * 
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    
    private TransactionId tid;
    private int tableid;
    private String tableAlias;
    private DbFileIterator fileIterator;
    private DbFile file;
    
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
    	this.tid = tid;
    	this.tableid = tableid;
    	this.tableAlias = tableAlias;
    	file = Database.getCatalog().getDbFile(this.tableid);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
    	return Database.getCatalog().getTableName(tableid);
    }
    
    /**
     * @return Return the alias of the table this operator scans. 
     * */
    public String getAlias()
    {
        // some code goes here
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
    	this.tableAlias = tableAlias;
    	this.tableid = tableid;
    	fileIterator = null;
    	
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	if (this.fileIterator == null) {
    		this.fileIterator = this.file.iterator(this.tid);
    		fileIterator.open();
    	} else {
    		throw new DbException("Already Open");
    	}
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     * 
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
    	TupleDesc td = this.file.getTupleDesc();
    	int length = td.numFields();
    	Type[] typeAr = new Type[length];
    	String [] fieldAr = new String[length];
    	for (int i = 0; i < length; i++){
    		fieldAr[i] = tableAlias + "." + td.getFieldName(i);
    		typeAr[i] = td.getFieldType(i);	 
    	}
    	
        return new TupleDesc(typeAr, fieldAr);

    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	return fileIterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
    	return fileIterator.next();
    }

    public void close() {
        // some code goes here
    	fileIterator = null;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
    	fileIterator.rewind();
    }
}
