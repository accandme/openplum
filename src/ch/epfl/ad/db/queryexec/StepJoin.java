package ch.epfl.ad.db.queryexec;

import java.util.Arrays;
import java.util.List;

public class StepJoin extends ExecStep {

	public List<String> joinTables;
	public List<String> joinConditions;
	public String outRelation;
	public StepPlace stepPlace;
	
	public StepJoin(List<String> joinTables, List<String> joinConditions, String outRelation, StepPlace stepPlace) {
		this.joinTables = joinTables;
		this.joinConditions = joinConditions;
		this.outRelation = outRelation;
		this.stepPlace = stepPlace;
	}
	
	@Override
	public String toString() {
		String on = "";
		String join = "CROSS JOIN ";
		if(joinConditions != null) {
			on = " ON " + Arrays.toString(joinConditions.toArray());
			join = "INNER JOIN ";
		}
		return "\n" + 
				"STEP " + join + Arrays.toString(joinTables.toArray()) +
				on +
				" INTO " + outRelation + 
				" ON " + stepPlace;
	}
	
}
