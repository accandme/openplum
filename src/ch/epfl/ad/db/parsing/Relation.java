package ch.epfl.ad.db.parsing;

/**
 * An SQL relation.
 * 
 * @author Artyom Stetsenko
 */
public abstract class Relation implements Operand {
	
	/**
	 * Relation alias.
	 */
	protected String alias;
	
	/**
	 * Setter for relation alias.
	 * 
	 * @param alias
	 *                new field alias
	 * @return this relation
	 */
	public abstract Relation setAlias(String alias);
	
	/**
	 * Getter for relation alias.
	 * 
	 * @return this relation's alias
	 */
	public String getAlias() {
		return this.alias;
	}
	
	public abstract String toUnaliasedString();
	public abstract String toString();
}
