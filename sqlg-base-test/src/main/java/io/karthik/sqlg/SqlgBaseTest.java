package io.karthik.sqlg;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.umlg.sqlg.structure.SqlgGraph;

public class SqlgBaseTest {
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void runTest() {
		Configuration c = new BaseConfiguration();
		c.addProperty("jdbc.url", "jdbc:postgresql://localhost:5433/sqlg-test");
		c.addProperty("jdbc.username", "postgres");
		c.addProperty("jdbc.password", "password");
		c.addProperty("implement.foreign.keys", true);
		c.addProperty("bulk.within.count", 2);
		c.addProperty("lock.timeout.minutes", 2);
		SqlgGraph g = SqlgGraph.open(c);


	    /*
											   Test Cluster	
													|
													|
											   Test Service	
													|
													|  
												 Test DB			
												/       \
											 /             \
										  /                   \
									   /                         \
									/                               \
								 /	    				               \
						  Test Schema1                    		  Test Schema2
						   /       \                                /       \
						 /           \                            /           \
					   /               \                		/               \ 
				  Table1                Table2              Table3               Table4
				  /    \                /    \              /    \               /    \
				 /      \              /      \            /      \             /      \
				/        \            /        \          /        \           /        \
			Column1    Column2    Column3   Column4   Column5   Column6    Column7   Column8

	        */

        Vertex cluster = g.addVertex(T.label, "Cluster", "name", "Test Cluster");
        Vertex service = g.addVertex(T.label, "Service", "name", "Test Service");
        Vertex database = g.addVertex(T.label, "Database", "name", "Test DB");
        Vertex schema1 = g.addVertex(T.label, "Schema", "name", "Test Schema1");
        Vertex schema2 = g.addVertex(T.label, "Schema", "name", "Test Schema2");
        Vertex table1 = g.addVertex(T.label, "Table", "name", "Table1");
        Vertex table2 = g.addVertex(T.label, "Table", "name", "Table2");
        Vertex table3 = g.addVertex(T.label, "Table", "name", "Table3");
        Vertex table4 = g.addVertex(T.label, "Table", "name", "Table4");
        Vertex column1 = g.addVertex(T.label, "Column", "name", "Column1");
        Vertex column2 = g.addVertex(T.label, "Column", "name", "Column2");
        Vertex column3 = g.addVertex(T.label, "Column", "name", "Column3");
        Vertex column4 = g.addVertex(T.label, "Column", "name", "Column4");
        Vertex column5 = g.addVertex(T.label, "Column", "name", "Column5");
        Vertex column6 = g.addVertex(T.label, "Column", "name", "Column6");
        Vertex column7 = g.addVertex(T.label, "Column", "name", "Column7");
        Vertex column8 = g.addVertex(T.label, "Column", "name", "Column8");
        
        cluster.addEdge("has_Service", service);
        service.addEdge("has_Database", database);
        database.addEdge("has_Schema", schema1);
        database.addEdge("has_Schema", schema2);
        schema1.addEdge("has_Table", table1);
        schema1.addEdge("has_Table", table2);
        schema2.addEdge("has_Table", table3);
        schema2.addEdge("has_Table", table4);
        table1.addEdge("has_Column", column1);
        table1.addEdge("has_Column", column2);
        table2.addEdge("has_Column", column3);
        table2.addEdge("has_Column", column4);
        table3.addEdge("has_Column", column5);
        table3.addEdge("has_Column", column6);
        table4.addEdge("has_Column", column7);
        table4.addEdge("has_Column", column8);
        
        g.tx().commit();
        
        String expected = "{Test Cluster={Test Service={Test DB={Test Schema1={Table1={Column1={}, Column2={}}}, Test Schema2={Table3={Column5={}, Column6={}}}}}}}"; 
        
        GraphTraversal<Vertex, Tree> traversal = g.traversal().V().has("public.Cluster","name","Test Cluster")
											   .out("has_Service").has("name","Test Service")
											   .out("has_Database").has("name","Test DB")
											   .union(
											        out("has_Schema").has("name",P.eq("Test Schema1")).out("has_Table").has("name",P.without("Table2")),
											        out("has_Schema").has("name",P.eq("Test Schema1")).out("has_Table").has("name",P.within("Table1")),
											        out("has_Schema").has("name",P.eq("Test Schema2")).out("has_Table").has("name",P.neq("Table4")))
											   .out("has_Column")
											   .range(0, 100).tree();
        
        Map<String, Object> actual = new LinkedHashMap<String, Object>();
        while (traversal.hasNext()) {
			traverseVertices(traversal.next(), actual);
		}
        System.out.println("Expected Result:" + expected);
    	System.out.println("Actual Result:" + actual.toString());
    	
        if (!expected.equals(actual.toString())) {
        	System.err.println("Failed!!");
        	throw new AssertionError();
        }
        System.out.println("Passed!!");
        
	}
	
	private void traverseVertices(Tree<Vertex> tree, Map<String, Object> map) {
		for (Map.Entry<Vertex, Tree<Vertex>> entry : tree.entrySet()) {
			Map<String, Object> temp1 = new LinkedHashMap<String, Object>();
			map.put(entry.getKey().value("name").toString(), temp1);
			if (entry.getValue() != null) {
				traverseVertices(entry.getValue(), temp1);
			}
		}
	}
}