package edu.jhuapl.blueprints.hbase;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;

public class HbaseVertex implements Vertex {
	
	private String id;
	private String graphName;
	private org.apache.hadoop.conf.Configuration hbaseConf;
	
	protected HbaseVertex() {}
	
	protected HbaseVertex(String id, String graphName, org.apache.hadoop.conf.Configuration hbaseConf) {
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
			get.addColumn(Bytes.toBytes("vertex"), Bytes.toBytes(key));
			Result result = hTable.get(get);
			if(result == null) {
				return null;
			}
			else {
				return (T) result.getValue(Bytes.toBytes("vertex"), Bytes.toBytes(key));
				
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
			get.addFamily(Bytes.toBytes("vertex"));
			Result result = hTable.get(get);
			if(result == null) {
				return null;
			}
			else {
				Set<String> stringKeys = new HashSet<String>();
				for(byte[] key : result.getFamilyMap(Bytes.toBytes("vertex")).keySet()) {
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
            delete.deleteFamily(Bytes.toBytes("vertex"));
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
            delete.deleteColumn(Bytes.toBytes("vertex"), Bytes.toBytes(key));
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
            delete.deleteColumn(Bytes.toBytes("vertex"), Bytes.toBytes(id));
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
			put1.add(Bytes.toBytes("vertex"), Bytes.toBytes(key), Bytes.toBytes(String.valueOf(value)));
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
			put1.add(Bytes.toBytes("vertex"), Bytes.toBytes(id), Bytes.toBytes(String.valueOf(value)));
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

	public Edge addEdge(String lable, Vertex inVertex) {

		String stringId = this.id+"_"+String.valueOf(inVertex.getId());
		
		HTable hTable = null;
		try {
			hTable = new HTable(this.hbaseConf, HbaseGraphUtils.GRAPHTABLENAME);
			Put put1 = new Put(Bytes.toBytes(this.graphName+"_"+stringId));
			put1.add(Bytes.toBytes("edge"), Bytes.toBytes("id"), Bytes.toBytes(stringId));
			put1.add(Bytes.toBytes("edge"), Bytes.toBytes("lable"), Bytes.toBytes(lable));
			put1.add(Bytes.toBytes("edge"), Bytes.toBytes("outVertex"), Bytes.toBytes(String.valueOf(this.id)));
			put1.add(Bytes.toBytes("edge"), Bytes.toBytes("inVertex"), Bytes.toBytes(String.valueOf(inVertex.getId())));
			hTable.put(put1);
			
			Put put2 = new Put(Bytes.toBytes(this.graphName+"_"+String.valueOf(this.id)));
			put2.add(Bytes.toBytes("vertex"), Bytes.toBytes("outEdge_"+stringId+"_"+lable), Bytes.toBytes(String.valueOf(inVertex.getId())));
			hTable.put(put2);
			
			Put put3 = new Put(Bytes.toBytes(this.graphName+"_"+String.valueOf(inVertex.getId())));
			put3.add(Bytes.toBytes("vertex"), Bytes.toBytes("inEdge_"+stringId+"_"+lable), Bytes.toBytes(String.valueOf(this.id)));
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

	public Iterable<Edge> getEdges(Direction dir, String... lables) {		
		switch (dir) {
			case IN:
				try {
					Scan scan = new Scan(Bytes.toBytes(this.graphName+"_"+this.id), HbaseGraphUtils.getEndKey(Bytes.toBytes(this.graphName+"_"+this.id)));
					scan.addFamily(Bytes.toBytes("vertex"));
					
					FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
					if(lables == null || lables.length <= 0) {
						filters.addFilter(new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator("inEdge_.*")));
					}
					else {
						for(String lable : lables) {
							filters.addFilter(new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator("inEdge_.*_"+lable)));
						}
					}
					
					scan.setFilter(filters);

					return new HbaseCellIterable<Edge>(scan, HbaseGraphUtils.GRAPHTABLENAME, hbaseConf) {
						@Override
						public Edge next(Cell cell) {
							String id = Bytes.toString(CellUtil.cloneQualifier(cell)).split("_")[1];							
							return new HbaseEdge(id, graphName, hbaseConf);
						}
					};		
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return null;
				}
				
			case OUT:
				try {
					Scan scan = new Scan(Bytes.toBytes(this.graphName+"_"+this.id), HbaseGraphUtils.getEndKey(Bytes.toBytes(this.graphName+"_"+this.id)));
					scan.addFamily(Bytes.toBytes("vertex"));
					
					FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
					if(lables == null || lables.length <= 0) {
						filters.addFilter(new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator("outEdge_.*")));
					}
					else {
						for(String lable : lables) {
							filters.addFilter(new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator("outEdge_.*_"+lable)));
						}
					}
					
					scan.setFilter(filters);

					return new HbaseCellIterable<Edge>(scan, HbaseGraphUtils.GRAPHTABLENAME, hbaseConf) {
						@Override
						public Edge next(Cell cell) {
							String id = Bytes.toString(CellUtil.cloneQualifier(cell)).split("_")[1];							
							return new HbaseEdge(id, graphName, hbaseConf);
						}
					};		
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return null;
				}
		default:
			try {
				Scan scan = new Scan(Bytes.toBytes(this.graphName+"_"+this.id), HbaseGraphUtils.getEndKey(Bytes.toBytes(this.graphName+"_"+this.id)));
				scan.addFamily(Bytes.toBytes("vertex"));
				
				FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
				if(lables == null || lables.length <= 0) {
					filters.addFilter(new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator(".*Edge_.*")));
				}
				else {
					for(String lable : lables) {
						filters.addFilter(new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator(".*Edge_.*_"+lable)));
					}
				}
				
				scan.setFilter(filters);

				return new HbaseCellIterable<Edge>(scan, HbaseGraphUtils.GRAPHTABLENAME, hbaseConf) {
					@Override
					public Edge next(Cell cell) {
						String id = Bytes.toString(CellUtil.cloneQualifier(cell)).split("_")[1];							
						return new HbaseEdge(id, graphName, hbaseConf);
					}
				};		
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}			
		}
	}

	public Iterable<Vertex> getVertices(Direction dir, String... lables) {
		switch (dir) {
		case IN:
			try {
				Scan scan = new Scan(Bytes.toBytes(this.graphName+"_"+this.id), HbaseGraphUtils.getEndKey(Bytes.toBytes(this.graphName+"_"+this.id)));
				scan.addFamily(Bytes.toBytes("vertex"));
				
				FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
				if(lables == null || lables.length <= 0) {
					filters.addFilter(new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator("inEdge_.*")));
				}
				else {
					for(String lable : lables) {
						filters.addFilter(new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator("inEdge_.*_"+lable)));
					}
				}
				
				scan.setFilter(filters);

				return new HbaseCellIterable<Vertex>(scan, HbaseGraphUtils.GRAPHTABLENAME, hbaseConf) {
					@Override
					public Vertex next(Cell cell) {
						String id = Bytes.toString(CellUtil.cloneValue(cell));							
						return new HbaseVertex(id, graphName, hbaseConf);
					}
				};		
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
			
		case OUT:
			try {
				Scan scan = new Scan(Bytes.toBytes(this.graphName+"_"+this.id), HbaseGraphUtils.getEndKey(Bytes.toBytes(this.graphName+"_"+this.id)));
				scan.addFamily(Bytes.toBytes("vertex"));
				
				FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
				if(lables == null || lables.length <= 0) {
					filters.addFilter(new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator("outEdge_.*")));
				}
				else {
					for(String lable : lables) {
						filters.addFilter(new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator("outEdge_.*_"+lable)));
					}
				}
				
				scan.setFilter(filters);

				return new HbaseCellIterable<Vertex>(scan, HbaseGraphUtils.GRAPHTABLENAME, hbaseConf) {
					@Override
					public Vertex next(Cell cell) {
						String id = Bytes.toString(CellUtil.cloneValue(cell));							
						return new HbaseVertex(id, graphName, hbaseConf);
					}
				};		
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
		default:
			try {
				Scan scan = new Scan(Bytes.toBytes(this.graphName+"_"+this.id), HbaseGraphUtils.getEndKey(Bytes.toBytes(this.graphName+"_"+this.id)));
				scan.addFamily(Bytes.toBytes("vertex"));
				
				FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
				if(lables == null || lables.length <= 0) {
					filters.addFilter(new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator(".*Edge_.*")));
				}
				else {
					for(String lable : lables) {
						filters.addFilter(new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator(".*Edge_.*_"+lable)));
					}
				}
				
				scan.setFilter(filters);
	
				return new HbaseCellIterable<Vertex>(scan, HbaseGraphUtils.GRAPHTABLENAME, hbaseConf) {
					@Override
					public Vertex next(Cell cell) {
						String id = Bytes.toString(CellUtil.cloneValue(cell));							
						return new HbaseVertex(id, graphName, hbaseConf);
					}
				};		
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}			
		}
	}

	public VertexQuery query() {
		 //TODO
		return null;
	}
}
