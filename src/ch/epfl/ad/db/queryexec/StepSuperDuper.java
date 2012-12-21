package ch.epfl.ad.db.queryexec;

public class StepSuperDuper extends ExecStep {

	public String fromRelation;
	public String toRelation;
	public String fromColumn;
	public String toColumn;
	public String outRelation;
	
	public StepSuperDuper(String fromRelation, String toRelation, String fromColumn, String toColumn, String outRelation) {
		this.fromRelation = fromRelation;
		this.toRelation = toRelation;
		this.fromColumn = fromColumn;
		this.toColumn = toColumn;
		this.outRelation = outRelation;
	}
	
	@Override
	public String toString() {
		return "\n" + 
				"STEP SUPERDUPER " + fromRelation + " (" + fromColumn + ")" +
				" TO " + toRelation + " (" + toColumn + ")" +
				" INTO " + outRelation;
	}
	
}
