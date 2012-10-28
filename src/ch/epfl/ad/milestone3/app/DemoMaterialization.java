package ch.epfl.ad.milestone3.app;

import java.sql.SQLException;

import ch.epfl.ad.AbstractQuery;
import ch.epfl.ad.db.DatabaseManager;
import ch.epfl.ad.milestone3.cube.Materializer;

public class DemoMaterialization extends AbstractQuery {

	@Override
	public void run(String[] args) throws SQLException, InterruptedException {
		DatabaseManager dbManager = createDatabaseManager(args[0]);
		dbManager.setResultShipmentBatchSize(5000);
		
		Materializer materializer = new Materializer(storageLimitCost, "materialized_views_catalog", 
		        "levels_arities", "view_fact_sales", CubeSettings.getDimensions(), 
		        CubeSettings.getFactFields(), dbManager, dbManager.getNodeNames().get(0));
		
		materializer.chooseAndMaterializeViews();
		dbManager.shutDown();
	}
	
	public static void main(String[] args) throws SQLException, InterruptedException {
		new DemoMaterialization().run(args);
	}	
}
