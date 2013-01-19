package ch.epfl.ad.db.queryexec;

import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.Arrays;
import java.util.List;

import ch.epfl.ad.app.TablePrinter;
import ch.epfl.ad.app.TempTableCleaner;
import ch.epfl.ad.bloomjoin.SuperDuper;
import ch.epfl.ad.db.AbstractDatabaseManager;
import ch.epfl.ad.db.DatabaseManager;
import ch.epfl.ad.db.queryexec.ExecStep;
import ch.epfl.ad.db.queryexec.StepGather;
import ch.epfl.ad.db.queryexec.StepRunSubq;
import ch.epfl.ad.db.queryexec.StepSuperDuper;
import ch.epfl.ad.db.queryexec.ExecStep.StepPlace;

public class StepExecutor {
	

	DatabaseManager dbManager;
	List<String> allNodes;
	
	List<ExecStep> execSteps;
	String finalResultRelationName;
	
	public StepExecutor(DatabaseManager dbManager, List<String> allNodes) {

		this.dbManager = dbManager;
		this.allNodes = allNodes;
		
	}
	
	public void setSteps(List<ExecStep> execSteps) {
		this.execSteps = execSteps;
	}

	public void executeSteps() throws SQLException, InterruptedException {
		if(execSteps == null) {
			throw new IllegalStateException("execSteps cannot be null, have you forgotten to call setExecStep?");
		}
		for(ExecStep step : execSteps) {
			if(step instanceof StepGather) {
				finalResultRelationName = ((StepGather) step).outRelation;
				ResultSet dummyRS = dbManager.fetch("SELECT * FROM " + ((StepGather) step).fromRelation + " WHERE 1=2", allNodes.get(0));
				String outSchema = AbstractDatabaseManager.tableSchemaFromMetaData(dummyRS.getMetaData());
				dbManager.execute("CREATE TABLE " + finalResultRelationName + " (" + outSchema + ")", allNodes.get(0));
				dbManager.execute("SELECT * FROM " + ((StepGather) step).fromRelation, allNodes, finalResultRelationName, allNodes.get(0));
			} else if(step instanceof StepRunSubq) {
				finalResultRelationName = ((StepRunSubq) step).outRelation;
				// TODO make the following line not return the whole results
				ResultSet dummyRS = dbManager.fetch(((StepRunSubq) step).query, allNodes.get(0));
				String outSchema = AbstractDatabaseManager.tableSchemaFromMetaData(dummyRS.getMetaData());
				if(((StepRunSubq) step).stepPlace == StepPlace.ON_WORKERS) {
					dbManager.execute("CREATE TABLE " + finalResultRelationName + " (" + outSchema + ")", allNodes);
					dbManager.execute(((StepRunSubq) step).query, allNodes, finalResultRelationName);
				} else if(((StepRunSubq) step).stepPlace == StepPlace.ON_MASTER) {
					dbManager.execute("CREATE TABLE " + finalResultRelationName + " (" + outSchema + ")", allNodes.get(0));
					dbManager.execute(((StepRunSubq) step).query, allNodes.get(0), finalResultRelationName);
				}
			} else if(step instanceof StepSuperDuper) {
				finalResultRelationName = ((StepSuperDuper) step).outRelation.getName();
				ResultSet dummyRS = dbManager.fetch("SELECT * FROM " + ((StepSuperDuper) step).fromRelation.getName() + " WHERE 1=2", allNodes.get(0));
				String outSchema = AbstractDatabaseManager.tableSchemaFromMetaData(dummyRS.getMetaData());
				SuperDuper sd = new SuperDuper(dbManager);
				List<String> fromNodes = null;
				if(((StepSuperDuper) step).distributeOnly) {
					fromNodes = Arrays.asList(new String[]{allNodes.get(0)});
				}else {
					fromNodes = allNodes;
				}
				dbManager.execute("CREATE TABLE " + finalResultRelationName + " (" + outSchema + ")", allNodes);
				sd.runSuperDuper(fromNodes, allNodes, ((StepSuperDuper) step).fromRelation.getName(), ((StepSuperDuper) step).toRelation.getName(), ((StepSuperDuper) step).fromColumn, ((StepSuperDuper) step).toColumn, finalResultRelationName);
				sd.shutDown();
			}
		}
	}
	
	public void printResult() throws SQLException, InterruptedException {
		if(finalResultRelationName == null) {
			throw new IllegalStateException("finalResultRelationName cannot be null, have you forgotten to call executeSteps?");
		}
		
		ResultSet result = dbManager.fetch("SELECT * FROM " + finalResultRelationName, allNodes.get(0));
		TablePrinter.printTableData(System.in, System.out, result);
	}

	public void cleanTempTables() throws SQLException, InterruptedException {
		
		TempTableCleaner ttc = new TempTableCleaner(dbManager);
		ttc.cleanTempTables(allNodes);
		ttc.shutDown();
		
	}

}
