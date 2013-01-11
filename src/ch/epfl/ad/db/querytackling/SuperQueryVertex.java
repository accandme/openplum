package ch.epfl.ad.db.querytackling;

import java.util.Set;

import ch.epfl.ad.db.parsing.QueryRelation;
import ch.epfl.ad.db.parsing.Relation;

public class SuperQueryVertex extends QueryVertex {
	
	public static final String ALIAS_ANONYMOUS_PREFIX = "_anonVertex_";
	
	private Relation query;
	private Set<QueryVertex> vertices;
	
	public SuperQueryVertex(Relation query, Set<QueryVertex> vertices) {
		this(query, vertices, query.getAlias());
	}
	
	public SuperQueryVertex(Relation query, Set<QueryVertex> vertices, String alias) {
		if (query == null) {
			throw new IllegalArgumentException("Supervertex query cannot be null.");
		}
		if (vertices == null || vertices.size() == 0) {
			throw new IllegalArgumentException("Supervertex must contain at least one vertex.");
		}
		this.query = query;
		this.alias = alias != null ? alias : ALIAS_ANONYMOUS_PREFIX + (int)(Math.random() * 1000000000);
		this.vertices = vertices;
	}
	
	public Relation getQuery() {
		return this.query;
	}
	
	public boolean isAggregate() {
		return (this.query instanceof QueryRelation) && ((QueryRelation)this.query).isAggregate();
	}
	
	public Set<QueryVertex> getVertices() {
		return this.vertices;
	}
	
	@Override
	public String toString() {
		StringBuilder string = new StringBuilder(this.alias).append("{");
		String prefix = "";
		for (QueryVertex vertex : this.vertices) {
			string.append(prefix);
			string.append(vertex);
			prefix = ", ";
		}
		string.append("}");
		return string.toString();
	}
}
