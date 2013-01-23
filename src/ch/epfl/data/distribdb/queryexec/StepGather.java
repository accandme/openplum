package ch.epfl.data.distribdb.queryexec;

/**
 * Class holding the information needed by 
 * the Gather elementary step
 * It stores the table name of the distributed 
 * table, and the name of the table, on the 
 * master, which will hold the collected tuples
 * 
 * @author Amer C (amer.chamseddine@epfl.ch)
 *
 */
public class StepGather extends ExecStep {

	/**
	 * Name of the distributed table to be gathered
	 */
	public final String fromRelation;
	/**
	 * Name of the table to hold the collected tuples 
	 * on the master
	 */
	public final String outRelation;
	
	/**
	 * Constructor - Initializes the object with 
	 * the needed stuff
	 * 
	 * @param fromRelation
	 * @param outRelation
	 */
	public StepGather(String fromRelation, String outRelation) {
		this.fromRelation = fromRelation;
		this.outRelation = outRelation;
	}
	
	/**
	 * Method to represent the internal state of 
	 * the object as a string
	 * This is useful for debugging purposes only
	 */
	@Override
	public String toString() {
		return "\n" + 
				"STEP GATHER " + fromRelation +
				" INTO " + outRelation;
	}
	
}
