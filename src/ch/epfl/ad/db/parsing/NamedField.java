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
	
	public String getField() {
		return this.fieldName;
	}
	
	@Override
	public NamedField setAlias(String alias) {
		this.alias = alias;
		return this;
	}
	
	@Override
	public String toString() {
		return String.format(
				"%s.%s",
				(this.relation.getAlias() != null ? this.relation.getAlias() : ((NamedRelation)this.relation).getName()),
				this.fieldName
				);
	}
}
