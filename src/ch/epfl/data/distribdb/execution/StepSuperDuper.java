package ch.epfl.data.distribdb.execution;

import ch.epfl.data.distribdb.parsing.NamedRelation;

/**
 * Class encoding the elementary SuperDuper step
 * It stores all the information required to perform 
 * a SuperDuper operation
 * Instances of this class are added by the 
 * GraphProcessor to the list of execution steps
 * Afterward this list is executed by the StepExecutor
 * 
 * @author Amer C (amer.chamseddine@epfl.ch)
 *
 */
public class StepSuperDuper extends ExecStep {

	/**
	 * Relation from which tuples will be shipped
	 */
	public final NamedRelation fromRelation;
	/**
	 * Relation to which tuples will be shipped
	 */
	public final NamedRelation toRelation;
	/**
	 * Name of column in fromRelation on which the join is performed 
	 */
	public final String fromColumn;
	/**
	 * Name of column in toRelation on which the join is performed
	 */
	public final String toColumn;
	/**
	 * Boolean specifying whether it is a full-blown SuperDuper 
	 * step (where tuples are shipped from all nodes to all nodes, 
	 * meaning that both tables fromRelation and toRelation 
	 * are distributed) or it is just a distribution 
	 * step (where tuples are shipped from the master to all worker 
	 * nodes, meaning that the fromRelation is not distributed and 
	 * resides on the master whereas the toRelation is distributed 
	 * on all nodes)
	 */
	public final boolean distributeOnly;
	/**
	 * Name of the temporary table that will contain the shipped 
	 * tuples
	 */
	public final NamedRelation outRelation;
	
	/**
	 * Constructor - Initializes the object with the different 
	 * parameters required to run a SuperDuper
	 * 
	 * @param fromRelation
	 * @param toRelation
	 * @param fromColumn
	 * @param toColumn
	 * @param distributeOnly
	 * @param outRelation
	 */
	public StepSuperDuper(NamedRelation fromRelation, NamedRelation toRelation, 
			String fromColumn, String toColumn, 
			boolean distributeOnly, NamedRelation outRelation) {
		this.fromRelation = fromRelation;
		this.toRelation = toRelation;
		this.fromColumn = fromColumn;
		this.toColumn = toColumn;
		this.distributeOnly = distributeOnly;
		this.outRelation = outRelation;
	}

	/**
	 * Prints the internal state of the object to string
	 * This is used for debugging purposes only
	 */
	@Override
	public String toString() {
		String step = "SUPERDUPER ";
		if(distributeOnly)
			step = "DISTRIBUTE ";
		return "\n" + 
				"STEP " + step + fromRelation + " (" + fromColumn + ")" +
				" TO " + toRelation + " (" + toColumn + ")" +
				" INTO " + outRelation;
	}
	
}
