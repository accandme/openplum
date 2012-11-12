package ch.epfl.ad.db.parsing;

public enum Operator {
	
	EXISTS (1),
	NOT_EXISTS (1),
	IN (2),
	NOT_IN (2),
	LESS_THAN (2),
	LESS_THAN_OR_EQUALS (2),
	EQUALS (2),
	GREATER_THAN (2),
	GREATER_THAN_OR_EQUALS (2);
	
	private final int numOperands;
	private Operator(int numOperands) {
		this.numOperands = numOperands;
	}
	
	public int getNumOperands() {
		return this.numOperands;
	}
}
