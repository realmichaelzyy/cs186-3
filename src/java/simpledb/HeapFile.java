package simpledb;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	private File file;
	private TupleDesc td;
	private FileChannel fileChannel;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	try{
	    	this.file = f;
	    	this.td = td;
	    	RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
	    	this.fileChannel = raf.getChannel();
    	}
    	catch (FileNotFoundException e) {
    		System.out.println("Couldn't find file.");
    	}
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes her
    	try {
	    	int pageNumber = pid.pageNumber();
	    	int offset = pageNumber * BufferPool.PAGE_SIZE;
	    	 
	    	ByteBuffer buffer = ByteBuffer.allocate(BufferPool.PAGE_SIZE);
	    	this.fileChannel.read(buffer, offset);
	    	
	    	return new HeapPage((HeapPageId) pid, buffer.array());
    	}
    	catch (IOException e) {
    		System.out.println("Could not read specified page.");
    		return null;
    	}
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
    	int pageNumber = page.getId().pageNumber();
    	int offset = pageNumber * BufferPool.PAGE_SIZE;
    	
    	try {
    		ByteBuffer buffer = ByteBuffer.wrap(page.getPageData());
    		this.fileChannel.write(buffer, offset);
    	} catch (IOException e) {
    		
    	}
    }

    /**
     * Returns the number of pages in this HeapFile.
     * @throws IOException 
     */
    public int numPages() {
        // some code goes here
    	try {
    		return (int) Math.ceil(this.fileChannel.size() / BufferPool.PAGE_SIZE);
    	}
    	catch (IOException e) {
    		System.out.println("Couldn't determine file size.");
    		return 0;
    	}
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
    	HeapPage freePage = (HeapPage) Database.getBufferPool().getPage(tid, getOpenPage(tid).getId(), Permissions.READ_WRITE);
    	freePage.insertTuple(t);
    	freePage.markDirty(true, tid);
    	
    	ArrayList<Page> modifiedPages = new ArrayList<Page>();
    	modifiedPages.add(freePage);
    	return modifiedPages;
    }
    
    private HeapPage getOpenPage(TransactionId tid) throws TransactionAbortedException, DbException, IOException {
    	for(int i=0; i<numPages(); i++) {
    		PageId pid = new HeapPageId(getId(),i);
    		HeapPage currPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE); 
    		if(currPage.getNumEmptySlots() != 0) {
    			return currPage;
    		}
    	}
    	
    	HeapPage newPage = new HeapPage(new HeapPageId(getId(), numPages()),new byte[BufferPool.PAGE_SIZE]);
    	writePage(newPage);
    	return newPage;
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
    	HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
    	page.deleteTuple(t);
    	page.markDirty(true, tid);
    	return page;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
    	return new HeapFileIterator(this, tid);
    }

    class HeapFileIterator implements DbFileIterator {
        
        private HeapFile heapFile;
        private TransactionId transactionId;
        private int tableId;
        private int currentPage;
        private Iterator<Tuple> pageIterator;

        HeapFileIterator(HeapFile hf, TransactionId tid) {
            this.heapFile = hf;
            this.transactionId = tid;
            this.tableId = hf.getId();
            this.currentPage = 0;
            this.pageIterator = null;
        }

        public void open() throws DbException, TransactionAbortedException {
        	 this.pageIterator = setPageIterator();
        }

        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (this.pageIterator == null) {
                return false;
            } else if (this.pageIterator.hasNext()) {
                return true;
            } else {
                this.currentPage++;
                if (this.currentPage < this.heapFile.numPages()) {
                	this.pageIterator = setPageIterator();
                    return this.pageIterator.hasNext();
                }
            }
            return false;
        }

        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (hasNext())
                return this.pageIterator.next();
            else
                throw new NoSuchElementException();
        }

        public void rewind() throws DbException, TransactionAbortedException {
        	this.currentPage = 0;
            this.pageIterator = setPageIterator();
        }

        public void close() {
        	this.pageIterator = null;
        }
        
        public Iterator<Tuple> setPageIterator() throws DbException, TransactionAbortedException {
            HeapPageId pid = new HeapPageId(this.tableId, this.currentPage);
            HeapPage p = (HeapPage)Database.getBufferPool().getPage(this.transactionId, pid, Permissions.READ_ONLY);
            return p.iterator();
        }
    }
}

