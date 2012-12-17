package ch.epfl.ad.db.parsing;

public enum Aggregate {
	
	SUM ("SUM"),
	AVG ("AVG"),
	COUNT ("COUNT"),
	MIN ("MIN"),
	MAX ("MAX");
	
	private static final Aggregate[] allValues = Aggregate.values();
	
	public static Aggregate forFunctionName(String functionName) {
		for (Aggregate aggregate : Aggregate.allValues) {
            if (aggregate.functionName.equalsIgnoreCase(functionName)) {
            	return aggregate;
            }
        }
		return null;
	}
	
	private final String functionName;
	
	private Aggregate(String functionName) {
		this.functionName = functionName;
	}
	
	@Override
	public String toString() {
		return this.functionName;
	}
}
