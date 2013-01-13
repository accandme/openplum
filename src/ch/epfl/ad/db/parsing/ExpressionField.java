package ch.epfl.ad.db.parsing;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * An SQL expression field.
 * 
 * @author Artyom Stetsenko
 */
public class ExpressionField extends Field {
	
	/**
	 * Placeholder string identifying the location of subfields in this field's expression string.
	 */
	public static String PLACEHOLDER = "--";
	
	/**
	 * The expression string.
	 */
	private String expression;
	
	/**
	 * Subfields that appear in the expression string.
	 */
	private List<Field> fields;
	
	/**
	 * Constructor of an expression field.
	 * 
	 * @param expression
	 *                the field's expression string
	 * @param field
	 *                the field appearing in the expression string
	 */
	public ExpressionField(String expression, Field field) {
		this(expression, new LinkedList<Field>(Arrays.asList(field)));
	}
	
	/**
	 * Constructor of an expression field.
	 * 
	 * @param expression
	 *                the field's expression string
	 * @param fields
	 *                fields appearing in the expression string
	 */
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
	
	/**
	 * Getter of this field's expression string.
	 * 
	 * @return this field's expression string
	 */
	public String getExpression() {
		return this.expression;
	}
	
	/**
	 * Getter of this field's subfields (that appear in the expression string).
	 * 
	 * @return this field's subfields
	 */
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
