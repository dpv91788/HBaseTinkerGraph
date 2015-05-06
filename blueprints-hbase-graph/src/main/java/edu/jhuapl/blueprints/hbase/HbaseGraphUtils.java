package edu.jhuapl.blueprints.hbase;

import org.apache.commons.codec.digest.DigestUtils;

public class HbaseGraphUtils {
	
	public final static String GRAPHTABLENAME = "blueprints_graphs";
	public final static String PROPERTIESTABLENAME = "blueprints_graphs_properties";

	public static byte[] getMd5Hash(String data) {
		return DigestUtils.md5(data);		
	}
	
	public static byte[] getEndKey(byte[] startKey) {
		startKey[startKey.length - 1]++;
		return startKey;
	}
}
