package ch.epfl.data.distribdb.tackling;

import java.util.Set;

import ch.epfl.data.distribdb.parsing.QueryRelation;

/**
 * An SQL graph vertex representing a subquery of the query encoded by the graph.
 * 
 * @author Artyom Stetsenko
 */
public class SuperQueryVertex extends QueryVertex {
	
	/**
	 * Prefix of aliases of anonymous super query vertices.
	 */
	public static final String ALIAS_ANONYMOUS_PREFIX = "_anonVertex_";
	
	/**
	 * Query represented by this vertex.
	 */
	private QueryRelation query;
	
	/**
	 * Set of subvertices of this vertex. 
	 */
	private Set<QueryVertex> vertices;
	
	/**
	 * Constructor of a super query vertex.
	 * 
	 * @param query
	 *                query represented by this vertex
	 * @param vertices
	 *                set of this vertex's subvertices
	 */
	public SuperQueryVertex(QueryRelation query, Set<QueryVertex> vertices) {
		this(query, vertices, query.getAlias());
	}
	
	/**
	 * Constructor of super query vertex.
	 * 
	 * @param query
	 *                query represented by this vertex
	 * @param vertices
	 *                set of this vertex's subvertices
	 * @param alias
	 *                this vertex's alias
	 */
	public SuperQueryVertex(QueryRelation query, Set<QueryVertex> vertices, String alias) {
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
	
	/**
	 * Getter of this vertex's query.
	 * 
	 * @return this vertex's query
	 */
	public QueryRelation getQuery() {
		return this.query;
	}
	
	/**
	 * Retrieves whether this vertex is aggregate.
	 * 
	 * @return true if this vertex's query is aggregate, false otherwise
	 */
	public boolean isAggregate() {
		return this.query.isAggregate();
	}
	
	/**
	 * Retrieves this vertex's subvertices.
	 * 
	 * @return this vertex's subvertices
	 */
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
