package ch.epfl.ad.db.parsing;

public enum Aggregate {
	
	SUM,
	AVG,
	COUNT,
	MIN,
	MAX;
	
	private static final Aggregate[] allValues = Aggregate.values();
	
	public static Aggregate forFunctionName(String functionName) {
		for (Aggregate aggregate : Aggregate.allValues) {
            if (aggregate.toString().equalsIgnoreCase(functionName)) {
            	return aggregate;
            }
        }
		return null;
	}
}
