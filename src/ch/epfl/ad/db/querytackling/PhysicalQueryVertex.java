package ch.epfl.ad.db.querytackling;

import ch.epfl.ad.db.parsing.NamedRelation;

public class PhysicalQueryVertex extends QueryVertex {
	
	private NamedRelation relation;
	
	public static PhysicalQueryVertex newInstance(String name) {
		return new PhysicalQueryVertex(new NamedRelation(name));
	}
	
	public PhysicalQueryVertex(NamedRelation relation) {
		if (relation == null) {
			throw new IllegalArgumentException("Physical query vertex constructor: arguments cannot be null.");
		}
		this.relation = relation;
		this.alias = relation.getAlias();
	}
	
	public String getName() {
		return this.relation.getName();
	}
	
	public NamedRelation getRelation() {
		return this.relation;
	}
	
	@Override
	public String toString() {
		return this.alias == null ? this.relation.getName() : this.alias + "[" + this.relation.getName() + "]";
	}
}
