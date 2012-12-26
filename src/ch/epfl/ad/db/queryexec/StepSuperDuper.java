package ch.epfl.ad.db.queryexec;

import ch.epfl.ad.db.parsing.NamedRelation;

public class StepSuperDuper extends ExecStep {

	public NamedRelation fromRelation;
	public NamedRelation toRelation;
	public String fromColumn;
	public String toColumn;
	public boolean distributeOnly;
	public NamedRelation outRelation;
	
	public StepSuperDuper(NamedRelation fromRelation, NamedRelation toRelation, String fromColumn, String toColumn, boolean distributeOnly, NamedRelation outRelation) {
		this.fromRelation = fromRelation;
		this.toRelation = toRelation;
		this.fromColumn = fromColumn;
		this.toColumn = toColumn;
		this.distributeOnly = distributeOnly;
		this.outRelation = outRelation;
	}
	
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
