package ch.epfl.ad.db.parsing;

public class Field implements Operand {
	
	private Relation relation;
	private String fieldName;
	
	public Field(Relation relation, String fieldName) {
		if (relation == null) {
			throw new IllegalArgumentException("Field reference operand relation cannot be null.");
		}
		if (!(relation instanceof NamedRelation) && relation.getAlias() == null) {
			throw new IllegalArgumentException("Field reference operand relation must be named or must have an alias.");
		}
		if (fieldName == null) {
			throw new IllegalArgumentException("Field reference operand field cannot be null.");
		}
		this.relation = relation;
		this.fieldName = fieldName;
	}
	
	public Relation getRelation() {
		return this.relation;
	}
	
	public String getField() {
		return this.fieldName;
	}
}
