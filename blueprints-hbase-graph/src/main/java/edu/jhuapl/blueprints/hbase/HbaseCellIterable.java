package edu.jhuapl.blueprints.hbase;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Element;

public abstract class HbaseCellIterable<T extends Element> implements CloseableIterable<T> {
	
	private ResultScanner scanner;
	private HTable hTable;
	
	public HbaseCellIterable(Scan scan, String table, Configuration conf) throws IOException {
		hTable = new HTable(conf, table);
		scanner = hTable.getScanner(scan);
	}
	
	public Iterator<T> iterator() {
		return new HbaseIterator();
	}
	
	public abstract T next(Cell cell);

	public void close() {
		if(hTable != null) {
			try {
				hTable.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	@Override
	protected void finalize() throws Throwable {
		close();
	}
	
	private class HbaseIterator implements Iterator<T> {
		private Result currResult;
		private Iterator<Cell> cellIt;
		
		private HbaseIterator() {}

		public boolean hasNext() {
			try {
				while(currResult == null) {
					currResult = scanner.next();
					if(currResult == null) {
						return false;
					}					
					
					if(!currResult.isEmpty()) {
						cellIt = currResult.listCells().iterator();	
						return true;
					}
					else {
						//The currResult had no cells get another result
						currResult = null;
					}
				}
				
				if(cellIt.hasNext()) {
					return true;
				}
				else {
					currResult = null;
					//Try for the next result and its cells or end of results
					while(currResult == null) {
						currResult = scanner.next();
						if(currResult == null) {
							return false;
						}					
						
						if(!currResult.isEmpty()) {
							cellIt = currResult.listCells().iterator();	
							return true;
						}
						else {
							//The currResult had no cells get another result
							currResult = null;
						}
					}
				}				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return false;
		}

		public T next() {
			return HbaseCellIterable.this.next(cellIt.next());
		}

		public void remove() {
			// TODO Auto-generated method stub
			
		}
	}
}