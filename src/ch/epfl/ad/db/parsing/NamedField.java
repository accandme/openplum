package ch.epfl.ad.db.parsing;

/**
 * An SQL named field (field of a relation).
 * 
 * @author Artyom Stetsenko
 */
public class NamedField extends Field {
	
	/**
	 * Relation to which this field belongs.
	 */
	private Relation relation;
	
	/**
	 * Name of this field.
	 */
	private String fieldName;
	
	/**
	 * Constructor of a named field.
	 * 
	 * @param relation
	 *                relation to which this field belongs
	 * @param fieldName
	 *                this field's name within the relation
	 */
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
	
	/**
	 * Constructor of a named field.
	 * 
	 * @param relation
	 *                relation to which this field belongs
	 * @param aliasedField
	 *                aliased field whose alias is this field's name
	 */
	public NamedField(Relation relation, Field aliasedField) {
		this(relation, aliasedField.getAlias());
	}
	
	/**
	 * Retrieves the relation to which this field belongs.
	 * 
	 * @return the relation to which this field belongs
	 */
	public Relation getRelation() {
		return this.relation;
	}
	
	/**
	 * Replaces this field's relation with a new relation.
	 * 
	 * @param newRelation relation with which to replace this field's relation
	 */
	public void replaceRelation(Relation newRelation) {
		this.relation = newRelation;
	}
	
	/**
	 * Retrieves this field's name.
	 * 
	 * @return this field's name
	 */
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
	public String toString() {
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
