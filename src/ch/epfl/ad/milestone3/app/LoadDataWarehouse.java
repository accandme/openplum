package ch.epfl.ad.milestone3.app;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import ch.epfl.ad.AbstractQuery;
import ch.epfl.ad.db.DatabaseManager;
import ch.epfl.ad.milestone3.cube.Materializer;

/**
 * Loads the Data Warehoues
 *  
 * @author Stanislav Peshterliev
 */
public class LoadDataWarehouse extends AbstractQuery {

	@Override
	public void run(String[] args) throws SQLException, InterruptedException {
		if (args.length < 1) {
		    System.out.println("Arguments: config-file");
		    System.exit(1);
		}
		
		System.out.println("Building Data Warehouse...");		
		
		DatabaseManager dbManager = createDatabaseManager(args[0]);
		dbManager.setResultShipmentBatchSize(5000);
		
		// empty the data warehouse
		dbManager.execute("DELETE FROM dim_parts", allNodes);
		dbManager.execute("DELETE FROM dim_suppliers", allNodes);
		dbManager.execute("DELETE FROM dim_customers", allNodes);
		dbManager.execute("DELETE FROM dim_time", allNodes);
		dbManager.execute("DELETE FROM fact_sales", allNodes);
		
		// clean up materialized views
		List<ResultSet> tables = dbManager.fetch("SHOW TABLES", allNodes);
		
		for (ResultSet table : tables) {
			while (table.next()) {
				String tableName = table.getString(1);
				if (tableName.matches("^view_(?:(?!fact).).*$")) {
					dbManager.execute("DROP TABLE IF EXISTS " + tableName, allNodes);
				}
			}
		}
		
		// load parts dimension
		dbManager.execute(String.format(
				"INSERT INTO dim_parts (id, mfgr, brand, name) " +
				"SELECT %s, %s, %s, %s " +
				"FROM part",
				escapeALL("p_partkey"), escapeALL("p_mfgr"), escapeALL("p_brand"), escapeALL("p_name")
		), allNodes);
		
		// load supplier dimension
		dbManager.execute(String.format(
				"INSERT INTO dim_suppliers (id, region, nation, name) " +
				"SELECT %s, %s, %s, %s " +
				"FROM supplier s JOIN nation n ON s_nationkey = n_nationkey JOIN region r ON r.r_regionkey = n.n_regionkey",
				escapeALL("s.s_suppkey"), escapeALL("r.r_name"), escapeALL("n.n_name"), escapeALL("s.s_name")
			), allNodes);

		// load customer dimension
		dbManager.execute(String.format(
				"INSERT INTO dim_customers (id, region, nation, name) " +
				"SELECT %s, %s, %s, %s " +
				"FROM customer c JOIN nation n ON c.c_nationkey = n.n_nationkey JOIN region r ON r.r_regionkey = n.n_regionkey",
				escapeALL("c.c_custkey"), escapeALL("r.r_name"), escapeALL("n.n_name"), escapeALL("c.c_name")
				), allNodes);	

		// load time dimension
		dbManager.execute("CREATE TABLE temp_time (o_orderdate DATE)", allNodes);
		dbManager.execute("INSERT INTO temp_time (o_orderdate) SELECT DISTINCT o_orderdate FROM orders", allNodes);
		
		dbManager.execute( 
				"INSERT INTO dim_time (id, year, month, day) " +
				"SELECT o_orderdate, EXTRACT(YEAR FROM o_orderdate), LPAD(EXTRACT(MONTH FROM o_orderdate), 2, '0'), LPAD(EXTRACT(DAY FROM o_orderdate), 2, '0') " +
				"FROM temp_time " +
				"GROUP BY o_orderdate ", allNodes);
		
		dbManager.execute("DROP TABLE IF EXISTS temp_time", allNodes);
		
		// load sales fact table
		dbManager.execute(
				"INSERT INTO fact_sales (part_id, customer_id, supplier_id, time_id, quantity, extendedprice) " +
				"SELECT l.l_partkey, o.o_custkey, l.l_suppkey, o.o_orderdate, SUM(l.l_quantity), SUM(l.l_extendedprice) " +
				"FROM lineitem l JOIN orders o ON l.l_orderkey = o.o_orderkey " +
				"GROUP BY l.l_partkey, o.o_custkey, l.l_suppkey, o.o_orderdate "
				, allNodes);
		
		// choose views to materialize and materialize them
		Materializer materializer = new Materializer(storageLimitCost, "materialized_views_catalog", 
				"levels_arities", "view_fact_sales", CubeSettings.getDimensions(), 
				CubeSettings.getFactFields(), dbManager, dbManager.getNodeNames().get(0));
		materializer.chooseAndMaterializeViews();
		
		dbManager.shutDown();
		
		System.out.println("Done.");
	}
	
	public static String escapeALL(String field) {
		return String.format("IF(STRCMP(%s, 'ALL'), %s, '\\\\ALL')", field, field);
	}
	
	public static void main(String[] args) throws SQLException, InterruptedException {
		new LoadDataWarehouse().run(args);
	}	
}
