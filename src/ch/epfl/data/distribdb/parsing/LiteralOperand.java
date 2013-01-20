package ch.epfl.data.distribdb.parsing;

/**
 * A literal operand of a Qualifier.
 * 
 * @author Artyom Stetsenko
 */
public class LiteralOperand implements Operand {
	
	/**
	 * The literal string.
	 */
	private String expression;
	
	/**
	 * Constructor of a literal operand.
	 * 
	 * @param expression
	 *                the operand's literal value
	 */
	public LiteralOperand(String expression) {
		if (expression == null) {
			throw new IllegalArgumentException("Expression operand cannot be null.");
		}
		this.expression = expression;
	}
	
	/**
	 * Getter of this operand's literal expression.
	 * 
	 * @return this operand's literal expression
	 */
	public String getExpression() {
		return this.expression;
	}
	
	@Override
	public String toString() {
		return this.expression;
	}
}
