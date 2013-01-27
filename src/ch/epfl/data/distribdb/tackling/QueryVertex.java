package ch.epfl.data.distribdb.tackling;

/**
 * A vertex of an SQL query graph, representing a relation of the query encoded by the graph.
 * 
 * @author Artyom Stetsenko
 */
public abstract class QueryVertex {
	
	/**
	 * Vertex alias.
	 */
	protected String alias;
	
	/**
	 * Getter for vertex alias.
	 * @return the vertex alias
	 */
	public String getAlias() {
		return this.alias;
	}
}
