package ch.epfl.data.distribdb.execution;

import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ch.epfl.data.distribdb.execution.ExecStep;
import ch.epfl.data.distribdb.execution.StepGather;
import ch.epfl.data.distribdb.execution.StepRunSubquery;
import ch.epfl.data.distribdb.execution.StepSuperDuper;
import ch.epfl.data.distribdb.execution.ExecStep.StepPlace;
import ch.epfl.data.distribdb.lowlevel.DatabaseManager;

import static ch.epfl.data.distribdb.app.AbstractApp.DEBUG;

/**
 * StepExecutor - Class to execute the distributed query plan
 * 
 * Should be constructed by giving it a handle to the DB manager,
 * table manager, and the list of all nodes
 * The executeSteps method should be called 
 * and passed the list of
 * steps constituting the distributed query plan to be executed
 * The method performs the actual 
 * execution, computes the final results, 
 * and returns them as a handle to ResultSet
 * 
 * @author Amer C (amer.chamseddine@epfl.ch)
 *
 */
public class StepExecutor {
	

	/**
	 * Handle to DB manager used to send queries to nodes
	 */
	DatabaseManager dbManager;
	/**
	 * Table Manager - responsible mainly for 
	 * generating unique names for temporary tables,
	 * keeping track of them,
	 * and deleting them at the end
	 */
	TableManager tableManager;
	/**
	 * List of all nodes
	 */
	List<String> allNodes;
	
	/**
	 * Constructor - Should pass it a handle to DB manager, table manager, 
	 * and the list of all nodes
	 * 
	 * @param DatabaseManager
	 * @param List<String> allNodes
	 */
	public StepExecutor(DatabaseManager dbManager, TableManager tableManager, List<String> allNodes) {

		this.dbManager = dbManager;
		this.tableManager = tableManager;
		this.allNodes = allNodes;
		
	}
	
	/** 
	 * Call this to perform the actual execution of the distributed 
	 * query plan that is given as argument
	 * It returns the ResultSet that contains the final results
	 * to be printed to the screen using the table printer
	 * 
	 * @param List<ExecStep> list of execution steps that should be executed 
	 * @returns ResultSet the final results 
	 * @throws SQLException
	 * @throws InterruptedException
	 */
	public ResultSet executeSteps(List<ExecStep> execSteps) throws SQLException, InterruptedException {
		if(execSteps == null) {
			throw new IllegalStateException("execSteps cannot be null!");
		}
		ExecStep finalStep = execSteps.get(execSteps.size() - 1);
		if(!(finalStep instanceof StepRunSubquery) || ((StepRunSubquery) finalStep).stepPlace != StepPlace.ON_MASTER) {
			throw new IllegalStateException("Bad final step: should be a query on master");
		}
		if(DEBUG) System.out.println("\nEXECUTION:");
		for(ExecStep step : execSteps) {
			if(step instanceof StepGather) {
				if(DEBUG) System.out.println("StepGather");
				dbManager.execute("SELECT * FROM " + ((StepGather) step).fromRelation, allNodes, 
						((StepGather) step).outRelation, allNodes.get(0));
			} else if(step instanceof StepRunSubquery) {
				if(DEBUG) System.out.println("StepRunSubquery");
				if(step == finalStep) {
					return dbManager.fetch(((StepRunSubquery) step).query, allNodes.get(0));
				}
				if(((StepRunSubquery) step).stepPlace == StepPlace.ON_WORKERS) {
					dbManager.execute(((StepRunSubquery) step).query, allNodes, ((StepRunSubquery) step).outRelation);
				} else if(((StepRunSubquery) step).stepPlace == StepPlace.ON_MASTER) {
					dbManager.execute(((StepRunSubquery) step).query, allNodes.get(0), ((StepRunSubquery) step).outRelation);
				}
			} else if(step instanceof StepSuperDuper) {
				if(DEBUG) System.out.println("StepSuperDuper");
				SuperDuper sd = new SuperDuper(dbManager);
				List<String> fromNodes = null;
				if(((StepSuperDuper) step).distributeOnly) {
					fromNodes = Arrays.asList(new String[]{allNodes.get(0)});
				}else {
					fromNodes = allNodes;
				}
				Map<String, String> bloomFilters = new HashMap<String, String>();
				for(String theNode : allNodes)
					bloomFilters.put(theNode, tableManager.generateTmpTblName("bloomfilter_" + theNode));
				sd.runSuperDuper(fromNodes, allNodes, 
						((StepSuperDuper) step).fromRelation.getName(), ((StepSuperDuper) step).toRelation.getName(), 
						((StepSuperDuper) step).fromColumn, ((StepSuperDuper) step).toColumn, 
						bloomFilters, ((StepSuperDuper) step).outRelation.getName());
				sd.shutDown();
			}
		}
		throw new IllegalStateException("StepExecutor::executeSteps This should never happen");
	}

}
