package ch.epfl.ad.db.parsing;

public class NamedRelation extends Relation {
	
	private String name;
	
	public NamedRelation(String name) {
		this(name, null);
	}
	
	public NamedRelation(String name, String alias) {
		if (name == null) {
			throw new IllegalArgumentException("Named relation name cannot be null.");
		}
		this.name = name;
		this.alias = alias;
	}
	
	public String getName() {
		return this.name;
	}
	
	@Override
	public String toString() {
		return this.name + (this.alias == null ? "" :  " " + this.alias);
	}
}
