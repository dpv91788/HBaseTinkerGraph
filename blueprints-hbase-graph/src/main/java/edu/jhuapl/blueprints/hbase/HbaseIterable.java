package edu.jhuapl.blueprints.hbase;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Element;

public abstract class HbaseIterable<T extends Element> implements CloseableIterable<T> {
	
	private ResultScanner scanner;
	private HTable hTable;
	
	public HbaseIterable(Scan scan, String table, Configuration conf) throws IOException {
		hTable = new HTable(conf, table);
		scanner = hTable.getScanner(scan);
	}
	
	public Iterator<T> iterator() {
		return new HbaseIterator();
	}
	
	public abstract T next(Result rresult);

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
		
		private HbaseIterator() {}

		public boolean hasNext() {
			try {
				currResult = scanner.next();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if(currResult == null) {
				return false;
			}
			else {
				return true;
			}
		}

		public T next() {
			return HbaseIterable.this.next(currResult);
		}

		public void remove() {
			// TODO Auto-generated method stub
			
		}
	}
}