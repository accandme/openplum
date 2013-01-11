package ch.epfl.ad.db.parsing;

public abstract class Relation implements Operand {
	
	protected String alias;
	
	public abstract Relation setAlias(String alias);
	public abstract String toString(QueryType type);
	
	public String getAlias() {
		return this.alias;
	}
	
	@Override
	public String toString() {
		return this.toString(QueryType.REGULAR);
	}
}
