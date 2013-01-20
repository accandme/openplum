package ch.epfl.data.distribdb.parsing;

/**
 * An SQL function field (non-aggregate function).
 * 
 * @author Artyom Stetsenko
 */
public class FunctionField extends Field {
	
	/**
	 * This field's function.
	 */
	private String function;
	
	/**
	 * The field to which the function is applied.
	 */
	private Field field;
	
	/**
	 * Constructor of a function field.
	 * 
	 * @param function
	 *                the field's function
	 * @param field
	 *                the field to which the function is applied
	 */
	public FunctionField(String function, Field field) {
		if (function == null) {
			throw new IllegalArgumentException("Function field function cannot be null.");
		}
		if (field == null) {
			throw new IllegalArgumentException("Function field field cannot be null.");
		}
		this.function = function;
		this.field = field;
	}
	
	/**
	 * Getter of this field's function.
	 * 
	 * @return this field's function
	 */
	public String getFunction() {
		return this.function;
	}
	
	/**
	 * Getter of the field to which this field's function is applied.
	 * 
	 * @return the field to which this field's function is applied
	 */
	public Field getField() {
		return this.field;
	}

	@Override
	public FunctionField setAlias(String alias) {
		this.alias = alias;
		return this;
	}
	
	@Override
	public boolean isAggregate() {
		return this.field.isAggregate();
	}
	
	@Override
	public String toFullIntermediateString(int i) {
		return this.isAggregate() ? this.field.toFullIntermediateString(i) : super.toFullIntermediateString(i);
	}
	
	@Override
	public String toFullFinalString(NamedRelation intermediateRelation, int i) {
		return this.isAggregate() ? this.function + "(" + this.field.toFullFinalString(intermediateRelation, i) + ")" : super.toFullFinalString(intermediateRelation, i);
	}
	
	@Override
	public String toString() {
		return this.function + "(" + this.field + ")";
	}
}
