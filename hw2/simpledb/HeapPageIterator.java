package simpledb;
import java.util.*;

public class HeapPageIterator implements Iterator<Tuple> {
	private HeapPage heapPage;
	private int numTuples;
	private int curTuple;
	
	public HeapPageIterator(HeapPage heapPage) {
		this.heapPage = heapPage;
		this.curTuple = 0;
		this.numTuples = this.heapPage.getNumAvailableTuples();
	}

	public boolean hasNext() {
		if (this.curTuple < this.numTuples)
			return true;
		else 
			return false;
	}
	
	public Tuple next() {
		return heapPage.tuples[this.curTuple++];
	}
	
	public void remove() throws UnsupportedOperationException {
		throw new UnsupportedOperationException("remove is unsupported!");
	}
}
