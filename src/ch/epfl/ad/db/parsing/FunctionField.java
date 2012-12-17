package ch.epfl.ad.db.parsing;

public class FunctionField extends Field {
	
	private String function;
	private Field field;
	
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
	
	public String getFunction() {
		return this.function;
	}
	
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
	public String toString() {
		return this.function + "(" + this.field + ")";
	}
}
