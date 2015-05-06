package edu.jhuapl.blueprints.hbase;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class HbaseEdge implements Edge {
	
	private String id;
	private String graphName;
	private org.apache.hadoop.conf.Configuration hbaseConf;
	
	protected HbaseEdge() {}
	
	protected HbaseEdge(String id, String graphName, org.apache.hadoop.conf.Configuration hbaseConf) {
		this.id = id;
		this.graphName = graphName;
		this.hbaseConf = hbaseConf;
	}


	public Object getId() {
		return this.id;
	}

	@SuppressWarnings("unchecked")
	public <T> T getProperty(String key) {
		HTable hTable = null;
		try {
			hTable = new HTable(this.hbaseConf, HbaseGraphUtils.GRAPHTABLENAME);
			Get get = new Get(Bytes.toBytes(this.graphName+"_"+id));
			get.addColumn(Bytes.toBytes("edge"), Bytes.toBytes(key));
			Result result = hTable.get(get);
			if(result == null) {
				return null;
			}
			else {
				return (T) result.getValue(Bytes.toBytes("edge"), Bytes.toBytes(key));
				
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
	}

	public Set<String> getPropertyKeys() {
		HTable hTable = null;
		try {
			hTable = new HTable(this.hbaseConf, HbaseGraphUtils.GRAPHTABLENAME);
			Get get = new Get(Bytes.toBytes(this.graphName+"_"+id));
			get.addFamily(Bytes.toBytes("edge"));
			Result result = hTable.get(get);
			if(result == null) {
				return null;
			}
			else {
				Set<String> stringKeys = new HashSet<String>();
				for(byte[] key : result.getFamilyMap(Bytes.toBytes("edge")).keySet()) {
					stringKeys.add(Bytes.toString(key));
				}
				return stringKeys;
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
	}

	public void remove() {
		//TODO have to remove data from properties table
		
		HTable hTable = null;
        try {
        	hTable = new HTable(this.hbaseConf, HbaseGraphUtils.GRAPHTABLENAME);
            Delete delete = new Delete(Bytes.toBytes(this.graphName+"_"+this.id));
            delete.deleteFamily(Bytes.toBytes("edge"));
            hTable.delete(delete);
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if(hTable != null) {
				try {
					hTable.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}           
        }
	}

	public <T> T removeProperty(String key) {
		T object = this.getProperty(key);
		
		HTable hTable = null;
        try {
        	hTable = new HTable(this.hbaseConf, HbaseGraphUtils.GRAPHTABLENAME);
            Delete delete = new Delete(Bytes.toBytes(this.graphName+"_"+this.id));
            delete.deleteColumn(Bytes.toBytes("edge"), Bytes.toBytes(key));
            hTable.delete(delete);             
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if(hTable != null) {
				try {
					hTable.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}           
        }
        
        try {       	
        	hTable = new HTable(this.hbaseConf, HbaseGraphUtils.PROPERTIESTABLENAME);
            Delete delete = new Delete(Bytes.toBytes(this.graphName+"_"+key));            
            delete.deleteColumn(Bytes.toBytes("edge"), Bytes.toBytes(id));
            hTable.delete(delete);            
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if(hTable != null) {
				try {
					hTable.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}           
        }
        return object;
	}

	public void setProperty(String key, Object value) {
		HTable hTable = null;
		
		//Write this property to the graphtable
		try {
			hTable = new HTable(this.hbaseConf, HbaseGraphUtils.GRAPHTABLENAME);
			Put put1 = new Put(Bytes.toBytes(this.graphName+"_"+id));
			put1.add(Bytes.toBytes("edge"), Bytes.toBytes(key), Bytes.toBytes(String.valueOf(value)));
			hTable.put(put1);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(hTable != null) {
				try {
					hTable.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}	
		
		//Write this property to the properties table
		try {
			hTable = new HTable(this.hbaseConf, HbaseGraphUtils.PROPERTIESTABLENAME);
			Put put1 = new Put(Bytes.toBytes(this.graphName+"_"+key));
			put1.add(Bytes.toBytes("edge"), Bytes.toBytes(id), Bytes.toBytes(String.valueOf(value)));
			hTable.put(put1);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(hTable != null) {
				try {
					hTable.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public String getLabel() {
		HTable hTable = null;
		try {
			hTable = new HTable(this.hbaseConf, HbaseGraphUtils.GRAPHTABLENAME);
			Get get = new Get(Bytes.toBytes(this.graphName+"_"+id));
			get.addColumn(Bytes.toBytes("edge"), Bytes.toBytes("lable"));
			Result result = hTable.get(get);
			if(result == null) {
				return null;
			}
			else {
				return Bytes.toString(result.getValue(Bytes.toBytes("edge"), Bytes.toBytes("lable")));				
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
	}

	public Vertex getVertex(Direction dir) throws IllegalArgumentException {
		HTable hTable = null;
		
		switch (dir) {
			case IN:				
				try {
					hTable = new HTable(this.hbaseConf, HbaseGraphUtils.GRAPHTABLENAME);
					Get get = new Get(Bytes.toBytes(this.graphName+"_"+id));
					get.addColumn(Bytes.toBytes("edge"), Bytes.toBytes("inVertex"));
					Result result = hTable.get(get);
					if(result == null) {
						return null;
					}
					else {
						String id = Bytes.toString(result.getValue(Bytes.toBytes("edge"), Bytes.toBytes("inVertex")));	
						return new HbaseVertex(id, graphName, hbaseConf);
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
			case OUT:
				try {
					hTable = new HTable(this.hbaseConf, HbaseGraphUtils.GRAPHTABLENAME);
					Get get = new Get(Bytes.toBytes(this.graphName+"_"+id));
					get.addColumn(Bytes.toBytes("edge"), Bytes.toBytes("outVertex"));
					Result result = hTable.get(get);
					if(result == null) {
						return null;
					}
					else {
						String id = Bytes.toString(result.getValue(Bytes.toBytes("edge"), Bytes.toBytes("outVertex")));	
						return new HbaseVertex(id, graphName, hbaseConf);
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
		default:
			throw new IllegalArgumentException("Direction BOTH not supported");
		}		
	}
}