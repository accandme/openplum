package ch.epfl.ad.db.parsing;

/**
 * Enum representing an SQL aggregate function.
 * 
 * @author Artyom Stetsenko
 */
public enum Aggregate {
	
	SUM,
	AVG,
	COUNT,
	MIN,
	MAX;
	
	private static final Aggregate[] allValues = Aggregate.values();
	
	/**
	 * Retrieves the enum for the specified function name.
	 * 
	 * @param functionName
	 *                  function name to return the enum for
	 * @return the enum representing functionName
	 */
	public static Aggregate forFunctionName(String functionName) {
		for (Aggregate aggregate : Aggregate.allValues) {
            if (aggregate.toString().equalsIgnoreCase(functionName)) {
            	return aggregate;
            }
        }
		return null;
	}
}
