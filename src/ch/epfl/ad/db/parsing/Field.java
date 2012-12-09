package ch.epfl.ad.db.parsing;

public abstract class Field {
	
	protected String alias;
	
	public abstract Field setAlias(String alias);
	
	public String getAlias() {
		return this.alias;
	}
	
	public String toAliasedString() {
		return this.alias != null ? this.alias : this.toString();
	}
	
	public String toFullString() {
		return this.alias != null ? String.format("%s AS %s", this, this.alias) : this.toString();
	}
}
