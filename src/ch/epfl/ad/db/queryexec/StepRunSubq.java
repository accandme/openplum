package ch.epfl.ad.db.queryexec;

public class StepRunSubq extends ExecStep {

	public String query;
	public boolean agg;
	public String outRelation;
	public StepPlace stepPlace;
	
	public StepRunSubq(String query, boolean agg, String outRelation, StepPlace stepPlace) {
		// TODO should add from node and to node
		this.query = query;
		this.agg = agg;
		this.outRelation = outRelation;
		this.stepPlace = stepPlace;
	}
	
	@Override
	public String toString() {
		return "\n" + 
				"STEP RUN " + (agg ? "AGG " : "") +
				"SUB-QUERY {" + query +
				"} INTO " + outRelation +
				" ON " + stepPlace;
	}
	
}
