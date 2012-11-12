package ch.epfl.ad.db.parsing;

public abstract class Relation implements Operand {
	
	protected String alias;
	
	public String getAlias() {
		return this.alias;
	}
}
