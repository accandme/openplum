package ch.epfl.ad.db.parsing;

/**
 * An SQL literal field.
 * 
 * @author Artyom Stetsenko
 */
public class LiteralField extends Field {
	
	/**
	 * The literal string.
	 */
	private String expression;
	
	/**
	 * Constructor of a literal field.
	 * 
	 * @param expression
	 *                the field's literal value
	 */
	public LiteralField(String expression) {
		if (expression == null) {
			throw new IllegalArgumentException("Expression operand cannot be null.");
		}
		this.expression = expression;
	}
	
	/**
	 * Getter of this field's literal expression.
	 * 
	 * @return this field's literal expression
	 */
	public String getExpression() {
		return this.expression;
	}

	@Override
	public LiteralField setAlias(String alias) {
		this.alias = alias;
		return this;
	}

	@Override
	public boolean isAggregate() {
		return false;
	}
	
	@Override
	public String toFinalString(NamedRelation intermediateRelation, String prefix, int i) {
		return this.expression;
	}
	
	@Override
	public String toAliasedIntermediateString(int i) {
		return "";
	}
	
	@Override
	public String toFullIntermediateString(int i) {
		return "";
	}
	
	@Override
	public String toString() {
		return this.expression;
	}
}
