package ch.epfl.ad.db.parsing;

/**
 * An SQL named relation (or physical relation).
 * 
 * @author Artyom Stetsenko
 */
public class NamedRelation extends Relation {
	
	/**
	 * This relation's name.
	 */
	private String name;
	
	/**
	 * Constructor of a named relation.
	 * 
	 * @param name
	 *                the relation name
	 */
	public NamedRelation(String name) {
		if (name == null) {
			throw new IllegalArgumentException("Named relation name cannot be null.");
		}
		this.name = name;
	}
	
	/**
	 * Getter for this relation's name.
	 * 
	 * @return this relation's name
	 */
	public String getName() {
		return this.name;
	}
	
	@Override
	public NamedRelation setAlias(String alias) {
		this.alias = alias;
		return this;
	}
	
	@Override
	public String toUnaliasedString() {
		return this.name;
	}
	
	@Override
	public String toString() {
		return this.alias != null ? String.format("%s %s", this.toUnaliasedString(), this.alias) : this.toUnaliasedString();
	}
}
