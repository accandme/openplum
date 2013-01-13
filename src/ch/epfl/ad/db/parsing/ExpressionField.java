package ch.epfl.ad.db.parsing;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class ExpressionField extends Field {
	
	public static String PLACEHOLDER = "--";
	
	private String expression;
	private List<Field> fields;
	
	public ExpressionField(String expression, Field field) {
		this(expression, new LinkedList<Field>(Arrays.asList(field)));
	}
	
	public ExpressionField(String expression, List<Field> fields) {
		if (expression == null) {
			throw new IllegalArgumentException("Expression field expression cannot be null.");
		}
		if (fields == null) {
			throw new IllegalArgumentException("Expression field fields list cannot be null.");
		}
		for (Field field : fields) {
			if (field == null) {
				throw new IllegalArgumentException("None of the expression field fields can be null.");
			}
		}
		this.expression = expression;
		this.fields = fields;
	}
	
	public String getExpression() {
		return this.expression;
	}
	
	public List<Field> getFields() {
		return this.fields;
	}

	@Override
	public ExpressionField setAlias(String alias) {
		this.alias = alias;
		return this;
	}
	
	@Override
	public boolean isAggregate() {
		for (Field field : this.fields) {
			if (field.isAggregate()) {
				return true;
			}
		}
		return false;
	}
	
	@Override
	public String toFullIntermediateString(int i) {
		if (!this.isAggregate()) return super.toFullIntermediateString(i);
		StringBuilder string = new StringBuilder();
		int ii = 0;
		String prefix = "";
		for (Field field : fields) {
			string.append(prefix);
			string.append(field.toFullIntermediateString(ALIAS_ANONYMOUS_PREFIX + i + "_", ++ii));
			prefix = ", ";
		}
		return string.toString();
	}
	
	@Override
	public String toFullFinalString(NamedRelation intermediateRelation, int i) {
		if (!this.isAggregate()) return super.toFullFinalString(intermediateRelation, i);
		String string = this.expression;
		int ii = 0;
		for (Field field : fields) {
			string = string.replaceAll(PLACEHOLDER + ++ii, field.toFinalString(intermediateRelation, ALIAS_ANONYMOUS_PREFIX + i + "_", ii));
		}
		return string;
	}
	
	@Override
	public String toString() {
		String string = this.expression;
		int i = 0;
		for (Field field : fields) {
			string = string.replaceAll(PLACEHOLDER + ++i, field.toString());
		}
		return string;
	}
}
