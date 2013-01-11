package ch.epfl.ad.db.parsing;

public class LiteralOperand implements Operand {
	
	private String expression;
	
	public LiteralOperand(String expression) {
		if (expression == null) {
			throw new IllegalArgumentException("Expression operand cannot be null.");
		}
		this.expression = expression;
	}
	
	public String getExpression() {
		return this.expression;
	}
	
	@Override
	public String toString(QueryType type) {
		return this.expression;
	}
}
