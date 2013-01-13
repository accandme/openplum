package ch.epfl.ad.db.querytackling;

/**
 * A vertex of an SQL query graph.
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
