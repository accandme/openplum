package ch.epfl.ad.db.queryexec;

/**
 * Parent class from which all classes that 
 * represent an elementary execution step inherit
 * 
 * @author Amer C (amer.chamseddine@epfl.ch)
 *
 */
public class ExecStep {

	/**
	 * Enumeration defining possible places where 
	 * an execution step is to be performed
	 * 
	 */
	public static enum StepPlace {
		ON_MASTER,
		ON_WORKERS
	}
	
}
