package ch.epfl.ad.bloomjoin;

import java.sql.SQLException;
import java.util.List;

import ch.epfl.ad.db.DatabaseManager;
import ch.epfl.ad.db.querytackling.QueryEdge;

public class DataMover {
	
	DatabaseManager dbManager;

	public DataMover(DatabaseManager manager) {
		dbManager = manager;
	}
	
	/*public void moveData(List<String> nodeIds, List<QueryEdge> qel) throws SQLException, InterruptedException {
		for(QueryEdge qe : qel) {
			SuperDuper sd = new SuperDuper(dbManager);
			sd.createHolder(nodeIds);
			qe.getStartPoint();
			qe.getEndPoint();
			//sd.runSuperDuper(nodeIds, fromRelation, toRelation, fromColumn, toColumn, fromSchema, outRelation);
			sd.deleteHolder(nodeIds);
		}
	}*/
	
}
