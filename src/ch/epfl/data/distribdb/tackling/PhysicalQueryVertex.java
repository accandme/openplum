package ch.epfl.data.distribdb.tackling;

import ch.epfl.data.distribdb.parsing.NamedRelation;

/**
 * A SQL query graph vertex representing a named (physical) relation.
 * 
 * @author Artyom Stetsenko
 */
public class PhysicalQueryVertex extends QueryVertex {
	
	/**
	 * Named relation represented by this vertex.
	 */
	private NamedRelation relation;
	
	/**
	 * Constructor of a new instance of physical query vertex.
	 * 
	 * @param name
	 *                name of named relation to construct a vertex for
	 * @return the constructed vertex
	 */
	public static PhysicalQueryVertex newInstance(String name) {
		return new PhysicalQueryVertex(new NamedRelation(name));
	}
	
	/**
	 * Constructor of physical query vertex.
	 * 
	 * @param relation
	 *                named relation represented by this vertex
	 */
	public PhysicalQueryVertex(NamedRelation relation) {
		if (relation == null) {
			throw new IllegalArgumentException("Physical query vertex constructor: arguments cannot be null.");
		}
		this.relation = relation;
		this.alias = relation.getAlias();
	}
	
	/**
	 * Retrieves this vertex's name.
	 * 
	 * @return the name of the named relation represented by this vertex
	 */
	public String getName() {
		return this.relation.getName();
	}
	
	/**
	 * Retrieves the relation represented by this vertex.
	 * 
	 * @return the relation represented by this vertex
	 */
	public NamedRelation getRelation() {
		return this.relation;
	}
	
	@Override
	public String toString() {
		return this.alias == null ? this.relation.getName() : this.alias + "[" + this.relation.getName() + "]";
	}
}
