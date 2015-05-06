/**
 * 
 */
package edu.jhuapl.blueprints.hbase;

import java.io.IOException;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;

/**
 * @author dpv91788
 *
 */
public class HbaseGraph implements Graph {
		
	private org.apache.commons.configuration.Configuration properties;
	private org.apache.hadoop.conf.Configuration hbaseConf;
//	private String zookeepers;
	private String graphName;
	
	public static HbaseGraph open(org.apache.commons.configuration.Configuration properties) throws HbaseGraphException, MasterNotRunningException, ZooKeeperConnectionException, IOException {
		return new HbaseGraph(properties);
	}
	
	public HbaseGraph(org.apache.commons.configuration.Configuration properties) throws HbaseGraphException, MasterNotRunningException, ZooKeeperConnectionException, IOException {
		this.properties = properties;
		this.validateConfig();
		this.createTables();
		
		//If the graph already exists in hbase open for use else create
		if(graphExists()) {
			this.open();
		}
		else {
			this.initilize();
		}
	}
	
	/**
	 * Opens a existing graph and read in information from Hbase
	 */
	private void open() {
		
	}
	
	/**
	 * Creates a new graph in hbase
	 * @throws IOException 
	 * 
	 * 
	 */
	private void initilize() throws IOException {
		HTable hTable = null;
		try {
			hTable = new HTable(this.hbaseConf, HbaseGraphUtils.GRAPHTABLENAME);
			Put put1 = new Put(Bytes.toBytes(this.graphName));
			put1.add(Bytes.toBytes("graphname"), Bytes.toBytes(""), Bytes.toBytes(this.graphName));
			hTable.put(put1);
		} finally {
			if(hTable != null)
				hTable.close();
		}		
	}
	
	private void validateConfig() throws HbaseGraphException {
		
//		this.zookeepers = this.properties.getString("blueprints.hbase.zookeepers");
//		if(this.zookeepers == null) {
//			throw new HbaseGraphException("No zookeepers set");
//		}
		this.graphName = this.properties.getString("blueprints.hbase.graphname");
		if(this.graphName == null) {
			throw new HbaseGraphException("No graphname set");
		}
		this.hbaseConf = HBaseConfiguration.create();
	}
	
	/**
	 * Creates the needed Hbase tables if they do not 
	 * already exist
	 * @throws IOException 
	 * @throws ZooKeeperConnectionException 
	 * @throws MasterNotRunningException 
	 * 
	 */
	private void createTables() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		HBaseAdmin admin = null;
		try {			
			admin = new HBaseAdmin(this.hbaseConf);
			if(!admin.tableExists(HbaseGraphUtils.GRAPHTABLENAME)) {
				HTableDescriptor table = new HTableDescriptor(TableName.valueOf(HbaseGraphUtils.GRAPHTABLENAME));
				table.addFamily(new HColumnDescriptor(Bytes.toBytes("vertex")));
				table.addFamily(new HColumnDescriptor(Bytes.toBytes("graphname")));
				table.addFamily(new HColumnDescriptor(Bytes.toBytes("edge")));
				
				admin.createTable(table);
			}
			
			if(!admin.tableExists(HbaseGraphUtils.PROPERTIESTABLENAME)) {
				HTableDescriptor table = new HTableDescriptor(TableName.valueOf(HbaseGraphUtils.PROPERTIESTABLENAME));
				table.addFamily(new HColumnDescriptor(Bytes.toBytes("vertex")));
				table.addFamily(new HColumnDescriptor(Bytes.toBytes("edge")));
				
				admin.createTable(table);
			}
		}
		finally {
			if(admin != null) 
				admin.close();
		}		
	}
	
	private boolean graphExists() throws IOException {
		HTable hTable = null;
		try {
			hTable = new HTable(this.hbaseConf, HbaseGraphUtils.GRAPHTABLENAME);
			Get get = new Get(HbaseGraphUtils.getMd5Hash(this.graphName));
			get.addFamily(Bytes.toBytes("graphname"));
			Result result = hTable.get(get);
			if(result.isEmpty()) {
				return false;
			}
			else {
				return true;
			}
		}
		finally {
			if(hTable != null)
				hTable.close();
		}
	}
	
	private byte[] getEndKey(byte[] startKey) {
		startKey[startKey.length - 1]++;
		return startKey;
	}

	public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex, String lable) {
		if (null == id)
		    throw ExceptionFactory.vertexIdCanNotBeNull();
		
		String stringId = String.valueOf(id);
		
		HTable hTable = null;
		try {
			hTable = new HTable(this.hbaseConf, HbaseGraphUtils.GRAPHTABLENAME);
			Put put1 = new Put(Bytes.toBytes(this.graphName+"_"+stringId));
			put1.add(Bytes.toBytes("edge"), Bytes.toBytes("id"), Bytes.toBytes(stringId));
			put1.add(Bytes.toBytes("edge"), Bytes.toBytes("lable"), Bytes.toBytes(lable));
			put1.add(Bytes.toBytes("edge"), Bytes.toBytes("outVertex"), Bytes.toBytes(String.valueOf(outVertex.getId())));
			put1.add(Bytes.toBytes("edge"), Bytes.toBytes("inVertex"), Bytes.toBytes(String.valueOf(inVertex.getId())));
			hTable.put(put1);
			
			Put put2 = new Put(Bytes.toBytes(this.graphName+"_"+String.valueOf(outVertex.getId())));
			put2.add(Bytes.toBytes("vertex"), Bytes.toBytes("outEdge_"+stringId+"_"+lable), Bytes.toBytes(String.valueOf(inVertex.getId())));
			hTable.put(put2);
			
			Put put3 = new Put(Bytes.toBytes(this.graphName+"_"+String.valueOf(inVertex.getId())));
			put3.add(Bytes.toBytes("vertex"), Bytes.toBytes("inEdge_"+stringId+"_"+lable), Bytes.toBytes(String.valueOf(outVertex.getId())));
			hTable.put(put3);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		} finally {
			if(hTable != null) {
				try {
					hTable.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}	
		
		return new HbaseEdge(stringId, graphName, hbaseConf);
	}

	public Vertex addVertex(Object id) {
		if (null == id)
		    throw ExceptionFactory.vertexIdCanNotBeNull();
		
		String stringId = String.valueOf(id);
		
		HTable hTable = null;
		try {
			hTable = new HTable(this.hbaseConf, HbaseGraphUtils.GRAPHTABLENAME);
			Put put1 = new Put(Bytes.toBytes(this.graphName+"_"+stringId));
			put1.add(Bytes.toBytes("vertex"), Bytes.toBytes("id"), Bytes.toBytes(stringId));
			hTable.put(put1);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		} finally {
			if(hTable != null) {
				try {
					hTable.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}	
		
		return new HbaseVertex(stringId, graphName, hbaseConf);
	}

	public Edge getEdge(Object id) {
		if (null == id)
		    throw ExceptionFactory.vertexIdCanNotBeNull();
		
		String stringId = String.valueOf(id);
		
		HTable hTable = null;
		try {
			hTable = new HTable(this.hbaseConf, HbaseGraphUtils.GRAPHTABLENAME);
			Get get = new Get(Bytes.toBytes(this.graphName+"_"+stringId));
			get.addColumn(Bytes.toBytes("edge"), Bytes.toBytes("id"));
			Result result = hTable.get(get);
			if(result == null || result.isEmpty()) {
				return null;
			}
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		} finally {
			if(hTable != null) {
				try {
					hTable.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}	
		
		return new HbaseEdge(stringId, graphName, hbaseConf);
	}

	public Iterable<Edge> getEdges() {
		try {
			Scan scan = new Scan(Bytes.toBytes(this.graphName), this.getEndKey(Bytes.toBytes(this.graphName)));
			scan.addColumn(Bytes.toBytes("edge"), Bytes.toBytes("id"));

			return new HbaseIterable<Edge>(scan, HbaseGraphUtils.GRAPHTABLENAME, hbaseConf) {
				@Override
				public Edge next(Result result) {			
					String id = Bytes.toString(result.getValue(Bytes.toBytes("edge"), Bytes.toBytes("id")));
					return new HbaseEdge(id, graphName, hbaseConf);
				}
			};		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	public Iterable<Edge> getEdges(String key, Object value) {
		try {
			Scan scan = new Scan(Bytes.toBytes(this.graphName+"_"+key), this.getEndKey(Bytes.toBytes(this.graphName+"_"+key)));
			scan.addFamily(Bytes.toBytes("edge"));
			scan.setFilter(new ValueFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(String.valueOf(value)))));

			return new HbaseIterable<Edge>(scan, HbaseGraphUtils.PROPERTIESTABLENAME, hbaseConf) {
				@Override
				public Edge next(Result result) {			
					for(Cell cell : result.listCells()) {
						String id = Bytes.toString(CellUtil.cloneQualifier(cell));
						return new HbaseEdge(id, graphName, hbaseConf);
					}
					return null;
				}
			};		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	public Features getFeatures() {
		Features f = new Features();
		f.ignoresSuppliedIds = false;
		f.isPersistent = true;
		f.supportsStringProperty = true;
		return f;
	}

	public Vertex getVertex(Object id) {
		if (null == id)
		    throw ExceptionFactory.vertexIdCanNotBeNull();
		
		String stringId = String.valueOf(id);
		
		HTable hTable = null;
		try {
			hTable = new HTable(this.hbaseConf, HbaseGraphUtils.GRAPHTABLENAME);
			Get get = new Get(Bytes.toBytes(this.graphName+"_"+stringId));
			get.addColumn(Bytes.toBytes("vertex"), Bytes.toBytes("id"));
			Result result = hTable.get(get);
			if(result == null || result.isEmpty()) {
				return null;
			}
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		} finally {
			if(hTable != null) {
				try {
					hTable.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}	
		
		return new HbaseVertex(stringId, graphName, hbaseConf);
	}

	public Iterable<Vertex> getVertices() {
		try {
			Scan scan = new Scan(Bytes.toBytes(this.graphName), this.getEndKey(Bytes.toBytes(this.graphName)));
			scan.addColumn(Bytes.toBytes("vertex"), Bytes.toBytes("id"));

			return new HbaseIterable<Vertex>(scan, HbaseGraphUtils.GRAPHTABLENAME, hbaseConf) {
				@Override
				public Vertex next(Result result) {			
					String id = Bytes.toString(result.getValue(Bytes.toBytes("vertex"), Bytes.toBytes("id")));
					return new HbaseVertex(id, graphName, hbaseConf);
				}
			};		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	public Iterable<Vertex> getVertices(String key, Object value) {
		try {
			Scan scan = new Scan(Bytes.toBytes(this.graphName+"_"+key), this.getEndKey(Bytes.toBytes(this.graphName+"_"+key)));
			scan.addFamily(Bytes.toBytes("vertex"));
			scan.setFilter(new ValueFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(String.valueOf(value)))));

			return new HbaseIterable<Vertex>(scan, HbaseGraphUtils.PROPERTIESTABLENAME, hbaseConf) {
				@Override
				public Vertex next(Result result) {			
					for(Cell cell : result.listCells()) {
						String id = Bytes.toString(CellUtil.cloneQualifier(cell));
						return new HbaseVertex(id, graphName, hbaseConf);
					}
					return null;
				}
			};		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	public GraphQuery query() {
		// TODO
		return null;
	}

	public void removeEdge(Edge edge) {
		edge.remove();		
	}

	public void removeVertex(Vertex vertex) {
		vertex.remove();
	}

	public void shutdown() {
		// TODO Auto-generated method stub		
	}	
	
	public static void main(String[] args) throws IOException, ConfigurationException, HbaseGraphException {
		
		Configuration conf = new PropertiesConfiguration();
		conf.addProperty("blueprints.hbase.graphname", "dave_test");
		
		HbaseGraph graph = new HbaseGraph(conf);
		Vertex vertex1 = graph.addVertex("1");
		vertex1.setProperty("Degree", "0");
		Vertex vertex2 = graph.addVertex("2");
		Vertex vertex3 = graph.addVertex("3");
		
		Edge edge1 = graph.addEdge("1", vertex1, vertex2, "edge1");
		Edge edge2 = graph.addEdge("2", vertex1, vertex3, "edge2");
		Edge edge3 = graph.addEdge("3", vertex3, vertex2, "edge3");
		edge3.setProperty("name", "test_edge3");
		System.out.println("outedge "+String.valueOf(edge1.getVertex(Direction.OUT).getId()));
		System.out.println("inedge "+String.valueOf(edge1.getVertex(Direction.IN).getId()));
		
		System.out.println("property [name] "+Bytes.toString((byte[])edge3.getProperty("name")));
		
		System.out.println("Edge4?"+graph.getEdge("4"));
		System.out.println("Edge2?"+graph.getEdge("2").getLabel());
		//graph.removeVertex(vertex1);
		//System.out.println(graph.getVertices().iterator().hasNext());
		
		for(Vertex vertex : graph.getVertices()) {
			System.out.println(vertex.getPropertyKeys());
		}
		
		for(Edge edge : graph.getEdges()) {
			System.out.println(edge.getLabel());
		}
		
		for(Edge edge : graph.getEdges("name", "test_edge3")) {
			System.out.println(edge.getLabel());
		}
		
		for(Vertex vertex : graph.getVertices("Degree", "0")) {
			System.out.println(Bytes.toString((byte[])vertex.getProperty("Degree")));
		}
		
		for(Edge edge : vertex2.getEdges(Direction.IN, new String[0])) {
			System.out.println(edge.getLabel());
		}
		
		for(Vertex vertex : vertex1.getVertices(Direction.OUT, new String[0])) {
			System.out.println(String.valueOf(vertex.getId()));
		}		
	}
}
