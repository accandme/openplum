package ch.epfl.ad.db.parsing;

public class NamedField extends Field implements Operand {
	
	private Relation relation;
	private String fieldName;
	
	public NamedField(Relation relation, String fieldName) {
		if (relation == null) {
			throw new IllegalArgumentException("Field relation cannot be null.");
		}
		if (!(relation instanceof NamedRelation) && relation.getAlias() == null) {
			throw new IllegalArgumentException("Field relation must be named or must have an alias.");
		}
		if (fieldName == null) {
			throw new IllegalArgumentException("Field name cannot be null.");
		}
		this.relation = relation;
		this.fieldName = fieldName;
	}
	
	public NamedField(Relation relation, Field aliasedField) {
		this(relation, aliasedField.getAlias());
	}
	
	public Relation getRelation() {
		return this.relation;
	}
	
	public void replaceRelation(Relation newRelation) {
		this.relation = newRelation;
	}
	
	public String getField() {
		return this.fieldName;
	}
	
	@Override
	public NamedField setAlias(String alias) {
		this.alias = alias;
		return this;
	}
	
	@Override
	public boolean isAggregate() {
		return false;
	}
	
	@Override
	public String toString(QueryType type) {
		return String.format(
				"%s.%s",
				(this.relation.getAlias() != null ? this.relation.getAlias() : ((NamedRelation)this.relation).getName()),
				this.fieldName
				);
	}
	
	@Override
	public String toFullFinalString(NamedRelation intermediateRelation, String prefix, int i) {
		return String.format("%s AS %s", this.toFinalString(intermediateRelation, prefix, i), (this.alias != null ? this.alias : this.fieldName));
	}
}
