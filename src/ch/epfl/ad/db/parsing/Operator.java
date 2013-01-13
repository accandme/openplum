package ch.epfl.ad.db.parsing;

/**
 * Enum representing the operator of a Qualifier, i.e. an SQL operator in WHERE or HAVING clauses.
 * 
 * @author Artyom Stetsenko
 */
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
	
	/**
	 * Retrieves the enum for the specified operator.
	 * 
	 * @param operatorString
	 *                operator to return the enum for
	 * @return the enum representing operatorString
	 */
	public static Operator forOperatorString(String operatorString) {
		for (Operator operator : Operator.allValues) {
            if (operator.operatorString.equalsIgnoreCase(operatorString)) {
            	return operator;
            }
        }
		return null;
	}
	
	/**
	 * The number of operands this operator must have.
	 */
	private final int numOperands;
	
	/**
	 * This operator as a string.
	 */
	private final String operatorString;
	
	/**
	 * Constructor of an operator.
	 * 
	 * @param numOperands
	 *                the number of operands the operator must have
	 * @param operatorString
	 *                the string representing the operator
	 */
	private Operator(int numOperands, String operatorString) {
		this.numOperands = numOperands;
		this.operatorString = operatorString;
	}
	
	/**
	 * Getter of this operator's required number of operands.
	 * 
	 * @return the number of operands this operator must have
	 */
	public int getNumOperands() {
		return this.numOperands;
	}
	
	@Override
	public String toString() {
		return this.operatorString;
	}
}
