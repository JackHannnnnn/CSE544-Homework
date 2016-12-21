package simpledb;

import java.io.Serializable;
import java.util.*;

public class HeapFileIterator implements DbFileIterator, Serializable {
	
	private static final long serialVersionUID = 1L;
	private int HeapFileId;
	private TransactionId tid;
	private int numPages;
	private boolean opened;
	
	private int curPageNum;
	private PageId curPageId;
	private HeapPage curPage;
	private Iterator<Tuple> curIterator;
	
	public HeapFileIterator(int HeapFileId, TransactionId tid, int numPages) {
		this.HeapFileId = HeapFileId;
		this.numPages = numPages;
		this.tid = tid;
		opened = false;
	}
	
	public void open() throws DbException, TransactionAbortedException {
		curPageNum = 0;
		curPageId = new HeapPageId(HeapFileId, curPageNum);
		curPage = (HeapPage) Database.getBufferPool().getPage(tid, curPageId, Permissions.READ_WRITE);
		curIterator = curPage.iterator();
		opened = true;
	}
	
	public boolean hasNext() throws DbException, TransactionAbortedException {
		if (opened) {
			if (curPageNum < numPages-1 || curIterator.hasNext()) {
				return true;
			} else
				return false;
		} else
			return false;
	}
	
	public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
		if (opened) {
			if (hasNext()) {
				if (curIterator.hasNext()) 
					return curIterator.next();
				else {
					curPageNum++;
					curPageId = new HeapPageId(HeapFileId, curPageNum);
					curPage = (HeapPage) Database.getBufferPool().getPage(tid, curPageId, Permissions.READ_WRITE);
					curIterator = curPage.iterator();
					return curIterator.next();
					
				}	
			} else 
				throw new NoSuchElementException();
		} else 
			throw new NoSuchElementException();
	}
	
	public void rewind() throws DbException, TransactionAbortedException {
		if (opened) {
			open();
		} else
			throw new DbException("");
	}
	
	public void close() {
		opened = false;
	}
}
