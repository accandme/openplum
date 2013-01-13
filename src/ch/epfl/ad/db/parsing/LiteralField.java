package ch.epfl.ad.db.parsing;

public class LiteralField extends Field {
	
	private String expression;
	
	public LiteralField(String expression) {
		if (expression == null) {
			throw new IllegalArgumentException("Expression operand cannot be null.");
		}
		this.expression = expression;
	}
	
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
