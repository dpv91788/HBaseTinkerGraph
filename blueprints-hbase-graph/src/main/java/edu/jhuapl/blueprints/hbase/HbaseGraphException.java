package edu.jhuapl.blueprints.hbase;

public class HbaseGraphException extends Exception {

	private static final long serialVersionUID = -1021793122542486909L;
	
	//Default constructor
	public HbaseGraphException() {}
	
	public HbaseGraphException(String message) {
		super(message);
	}
	
	public HbaseGraphException(Throwable cause) {
		super(cause);
	}
	
	public HbaseGraphException(String message, Throwable cause) {
		super(message, cause);
	}
}