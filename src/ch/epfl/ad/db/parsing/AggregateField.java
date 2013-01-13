package ch.epfl.ad.db.parsing;

public class AggregateField extends Field implements Operand {
	
	public static final String SUFFIX_INTERMEDIATE = "_intermediate";
	public static final String SUFFIX_FINAL = "_final";
	
	private Aggregate aggregate;
	private Field field;
	
	public AggregateField(Aggregate aggregate, Field field) {
		if (aggregate == null) {
			throw new IllegalArgumentException("Aggregate field aggregate cannot be null.");
		}
		if (field == null) {
			throw new IllegalArgumentException("Aggregate field field cannot be null.");
		}
		this.aggregate = aggregate;
		this.field = field;
	}
	
	public Aggregate getAggregate() {
		return this.aggregate;
	}
	
	public Field getField() {
		return this.field;
	}
	
	@Override
	public AggregateField setAlias(String alias) {
		this.alias = alias;
		return this;
	}
	
	@Override
	public boolean isAggregate() {
		return true;
	}
	
	@Override
	public String toString() {
		return this.aggregate + "(" + this.field + ")";
	}
	
	@Override
	public String toIntermediateString() {
		return this.aggregate + SUFFIX_INTERMEDIATE + "(" + this.field.toIntermediateString() + ")";
	}
	
	@Override
	public String toFinalString(NamedRelation intermediateRelation, String prefix, int i) {
		return this.aggregate + SUFFIX_FINAL + "(" + (this.alias != null ? this.alias : this.field.toFinalString(intermediateRelation, prefix, i)) + ")";
	}
}
