package ch.epfl.ad.db.parsing;

public class ExpressionOperand implements Operand {
	
	private String expression;
	
	public ExpressionOperand(String expression) {
		if (expression == null) {
			throw new IllegalArgumentException("Expression operand cannot be null.");
		}
		this.expression = expression;
	}
	
	public String getExpression() {
		return this.expression;
	}
}
