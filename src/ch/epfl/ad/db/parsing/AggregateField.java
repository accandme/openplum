package ch.epfl.ad.db.parsing;

public class AggregateField extends Field implements Operand {
	
	private Aggregate aggregate;
	private NamedField field;
	
	public AggregateField(Aggregate aggregate, NamedField field) {
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
	
	public NamedField getField() {
		return this.field;
	}
	
	@Override
	public AggregateField setAlias(String alias) {
		this.alias = alias;
		return this;
	}
	
	@Override
	public String toString() {
		return this.aggregate + "(" + this.field + ")";
	}
}
