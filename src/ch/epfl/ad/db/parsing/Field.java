package ch.epfl.ad.db.parsing;

public abstract class Field implements Operand {
	
	public static final String ALIAS_ANONYMOUS_PREFIX = "f";
	
	protected String alias;
	
	public abstract Field setAlias(String alias);
	public abstract boolean isAggregate();
	
	public String getAlias() {
		return this.alias;
	}
	
	public abstract String toString(QueryType type);
	
	@Override
	public String toString() {
		return this.toString(QueryType.REGULAR);
	}
	
	public String toIntermediateString() {
		return this.toString();
	}
	
	public String toFinalString(NamedRelation intermediateRelation, int i) {
		return this.toFinalString(intermediateRelation, ALIAS_ANONYMOUS_PREFIX, i);
	}
	
	public String toFinalString(NamedRelation intermediateRelation, String prefix, int i) {
		return String.format(
				"%s.%s",
				intermediateRelation.getAlias() != null ? intermediateRelation.getAlias() : intermediateRelation.getName(),
				prefix + i
				);
	}
	
	public String toAliasedString(QueryType type) {
		return this.alias != null ? this.alias : this.toString(type);
	}
	
	public String toAliasedIntermediateString(int i) {
		if (this.isAggregate()) throw new IllegalStateException("Cannot convert an aggregate field to an aliased intermediate string.");
		return this.alias != null ? this.alias : ALIAS_ANONYMOUS_PREFIX + i;
	}
	
	public String toAliasedFinalString(NamedRelation intermediateRelation, int i) {
		return this.alias != null ? this.alias : this.toFinalString(intermediateRelation, i);
	}
	
	public String toFullString(QueryType type) {
		return this.alias != null ? String.format("%s AS %s", this.toString(type), this.alias) : this.toString(type);
	}
	
	public String toFullIntermediateString(int i) {
		return this.toFullIntermediateString(ALIAS_ANONYMOUS_PREFIX, i);
	}
	
	public String toFullIntermediateString(String prefix, int i) {
		return String.format("%s AS %s", this.toIntermediateString(), this.alias != null ? this.alias : prefix + i);
	}
	
	public String toFullFinalString(NamedRelation intermediateRelation, int i) {
		return this.toFullFinalString(intermediateRelation, ALIAS_ANONYMOUS_PREFIX, i);
	}
	
	public String toFullFinalString(NamedRelation intermediateRelation, String prefix, int i) {
		return this.alias != null ?
				String.format("%s AS %s", this.toFinalString(intermediateRelation, prefix, i), this.alias) :
					this.toFinalString(intermediateRelation, prefix, i);
	}
}
