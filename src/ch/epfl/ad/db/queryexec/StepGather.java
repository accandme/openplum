package ch.epfl.ad.db.queryexec;

public class StepGather extends ExecStep {

	public String fromRelation;
	public String outRelation;
	
	public StepGather(String fromRelation, String outRelation) {
		this.fromRelation = fromRelation;
		this.outRelation = outRelation;
	}
	
	@Override
	public String toString() {
		return "\n" + 
				"STEP GATHER " + fromRelation +
				" INTO " + outRelation;
	}
	
}
