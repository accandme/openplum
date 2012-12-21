package ch.epfl.ad.db.queryexec;

public class StepAggregate extends ExecStep {

	public String aggQuery;
	public String outRelation;
	public StepPlace stepPlace;
	
	public StepAggregate(String aggQuery, String outRelation, StepPlace stepPlace) {
		this.aggQuery = aggQuery;
		this.outRelation = outRelation;
		this.stepPlace = stepPlace;
	}
	
	@Override
	public String toString() {
		return "\n" + 
				"STEP AGGREGATE " + aggQuery + 
				" INTO " + outRelation + 
				" ON " + stepPlace;
	}
	
}
