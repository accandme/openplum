package ch.epfl.data.zad.milestone3.app;

import java.sql.SQLException;

import ch.epfl.data.distribdb.app.AbstractQuery;
import ch.epfl.data.distribdb.lowlevel.DatabaseManager;
import ch.epfl.data.zad.milestone3.cube.Materializer;

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
