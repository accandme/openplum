package ch.epfl.ad.db.parsing;

public class NamedRelation extends Relation {
	
	private String name;
	
	public NamedRelation(String name) {
		if (name == null) {
			throw new IllegalArgumentException("Named relation name cannot be null.");
		}
		this.name = name;
	}
	
	public String getName() {
		return this.name;
	}
	
	@Override
	public NamedRelation setAlias(String alias) {
		this.alias = alias;
		return this;
	}
	
	@Override
	public String toString(QueryType type) {
		return this.name + (this.alias == null ? "" :  " " + this.alias);
	}
}
