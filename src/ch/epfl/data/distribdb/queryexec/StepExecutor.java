package ch.epfl.data.distribdb.queryexec;

import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.Arrays;
import java.util.List;

import ch.epfl.data.distribdb.app.TablePrinter;
import ch.epfl.data.distribdb.lowlevel.DatabaseManager;
import ch.epfl.data.distribdb.queryexec.ExecStep;
import ch.epfl.data.distribdb.queryexec.StepGather;
import ch.epfl.data.distribdb.queryexec.StepRunSubquery;
import ch.epfl.data.distribdb.queryexec.StepSuperDuper;
import ch.epfl.data.distribdb.queryexec.ExecStep.StepPlace;

/**
 * StepExecutor - Class to execute the distributed query plan
 * 
 * Should be constructed by giving it a handle to the DB manager
 * and the list of all nodes
 * The setSteps method should be called and passed the list of
 * steps constituting the distributed query plan to be executed
 * Then the executeSteps is called to perform the actual 
 * execution; this method computes the final results but keeps 
 * it stored in its internal state
 * The printResult method should be called, after executeSteps, 
 * in order to print the final result stored in the internal 
 * state 
 * Finally cleanTempTables should be called to cleanup the 
 * temporary tables which were created in the various
 * underlying databases
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
	 * List of all nodes
	 */
	List<String> allNodes;
	
	/**
	 * List of steps constituting the distributed query plan 
	 */
	List<ExecStep> execSteps;
	/**
	 * Result set of the final result generated on the master node
	 */
	ResultSet finalResultSet;

	/**
	 * Boolean specifying whether we should print 
	 * debugging logs to the console output
	 */
	public static final boolean DEBUG = false;

	/**
	 * Constructor - Should pass it a handle to DB manager
	 * and the list of all nodes
	 * 
	 * @param DatabaseManager
	 * @param List<String> allNodes
	 */
	public StepExecutor(DatabaseManager dbManager, List<String> allNodes) {

		this.dbManager = dbManager;
		this.allNodes = allNodes;
		
	}
	
	/**
	 * Call this to set the list of steps constituting the distributed 
	 * query plan
	 * This list will be stored in an internal state and executeSteps 
	 * will later act upon it 
	 * 
	 * @param List<ExecStep> execSteps
	 */
	public void setSteps(List<ExecStep> execSteps) {
		this.execSteps = execSteps;
	}

	/** 
	 * Call this to perform the actual execution of the distributed 
	 * query plan that was given using setSteps
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */
	public void executeSteps() throws SQLException, InterruptedException {
		if(execSteps == null)
			throw new IllegalStateException("execSteps cannot be null, have you forgotten to call setExecStep?");
		ExecStep finalStep = execSteps.get(execSteps.size() - 1);
		if(!(finalStep instanceof StepRunSubquery) || ((StepRunSubquery) finalStep).stepPlace != StepPlace.ON_MASTER)
			throw new IllegalStateException("Bad final step: should be a query on master");
		if(DEBUG) System.out.println("\nEXECUTION:");
		for(ExecStep step : execSteps) {
			if(step instanceof StepGather) {
				if(DEBUG) System.out.println("StepGather");
				dbManager.execute("SELECT * FROM " + ((StepGather) step).fromRelation, allNodes, 
						((StepGather) step).outRelation, allNodes.get(0));
			} else if(step instanceof StepRunSubquery) {
				if(DEBUG) System.out.println("StepRunSubquery");
				if(step == finalStep) {
					finalResultSet = dbManager.fetch(((StepRunSubquery) step).query, allNodes.get(0));
					return;
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
				sd.runSuperDuper(fromNodes, allNodes, ((StepSuperDuper) step).fromRelation.getName(), 
						((StepSuperDuper) step).toRelation.getName(), ((StepSuperDuper) step).fromColumn, 
						((StepSuperDuper) step).toColumn, ((StepSuperDuper) step).outRelation.getName());
				sd.shutDown();
			}
		}
	}
	
	/**
	 * Call this to print the final result of the query
	 * The result is stored in a temporary table and the name of the 
	 * table is stored internally in the state of this object
	 * Make sure you have executed the query before printing it
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */
	public void printResult() throws SQLException, InterruptedException {
		if(finalResultSet == null) {
			throw new IllegalStateException("finalResultSet cannot be null, have you forgotten to call executeSteps?");
		}
		
		TablePrinter.printTableData(System.in, System.out, finalResultSet);
	}

	/**
	 * This should be called after executing the query and 
	 * printing the results
	 * It cleans the temporary tables which were created in 
	 * the various underlying non-distributed DBMSs
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */
	public void cleanTempTables() throws SQLException, InterruptedException {
		
		TempTableCleaner ttc = new TempTableCleaner(dbManager);
		ttc.cleanTempTables(allNodes);
		ttc.shutDown();
		
	}

}
