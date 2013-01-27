package ch.epfl.data.distribdb.execution;

/**
 * Class to store information needed to perform 
 * the RunSubQuery elementary step
 * It stores the string constituting the 
 * actual (sub)query to be executed,
 * the table in which to store the results, 
 * the place where to run the query,
 * and whether it is aggregate or not
 *  
 * @author Amer C (amer.chamseddine@epfl.ch)
 *
 */
public class StepRunSubquery extends ExecStep {

	/**
	 * String representing the sub-query to be run
	 */
	public final String query;
	/**
	 * Boolean representing whether or not the 
	 * sub-query is aggregate 
	 */
	public final boolean agg;
	/**
	 * String defining the name of the table which will hold 
	 * the tuples of the result of the execution
	 */
	public final String outRelation;
	/**
	 * Place where the sub-query will be executed
	 * Can be either on workers or on master
	 */
	public final StepPlace stepPlace;
	
	/**
	 * Constructor - Initializes object with all the required parameters
	 *  
	 * @param query
	 * @param agg
	 * @param outRelation
	 * @param stepPlace
	 */
	public StepRunSubquery(String query, boolean agg, String outRelation, StepPlace stepPlace) {
		this.query = query;
		this.agg = agg; // TODO not used anywhere, should remove
		this.outRelation = outRelation;
		this.stepPlace = stepPlace;
	}
	
	/**
	 * Represents the internal structure of the 
	 * object as a string
	 * This is used solely for debugging purposes 
	 */
	@Override
	public String toString() {
		return "\n" + 
				"STEP RUN " + (agg ? "AGG " : "") +
				"SUB-QUERY {" + query +
				"} INTO " + outRelation +
				" ON " + stepPlace;
	}
	
}
