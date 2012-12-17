package ch.epfl.ad.db.parsing;

public enum Operator {
	
	EXISTS (1, "EXISTS"),
	NOT_EXISTS (1, "NOT EXISTS"),
	IN (2, "IN"),
	NOT_IN (2, "NOT IN"),
	LESS_THAN (2, "<"),
	LESS_THAN_OR_EQUALS (2, "<="),
	EQUALS (2, "="),
	NOT_EQUALS (2, "<>"),
	GREATER_THAN (2, ">"),
	GREATER_THAN_OR_EQUALS (2, ">="),
	BETWEEN (3, "BETWEEN");
	
	private static final Operator[] allValues = Operator.values();
	
	public static Operator forOperatorString(String operatorString) {
		for (Operator operator : Operator.allValues) {
            if (operator.operatorString.equalsIgnoreCase(operatorString)) {
            	return operator;
            }
        }
		return null;
	}
	
	private final int numOperands;
	private final String operatorString;
	
	private Operator(int numOperands, String operatorString) {
		this.numOperands = numOperands;
		this.operatorString = operatorString;
	}
	
	public int getNumOperands() {
		return this.numOperands;
	}
	
	@Override
	public String toString() {
		return this.operatorString;
	}
}
