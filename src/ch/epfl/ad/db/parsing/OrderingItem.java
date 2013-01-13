package ch.epfl.ad.db.parsing;

/**
 * An SQL ORDER BY clause field.
 * 
 * @author Artyom Stetsenko
 */
public class OrderingItem {
	
	/**
	 * The field of this item.
	 */
	private Field field;
	
	/**
	 * The sorting type of the field. 
	 */
	private OrderingType orderingType;
	
	/**
	 * Constructor of an ORDER BY field.
	 * 
	 * @param field
	 *                field of this item
	 */
	public OrderingItem(Field field) {
		this(field, null);
	}
	
	/**
	 * Constructor of an ORDER BY field.
	 * 
	 * @param field
	 *                field of this item
	 * @param orderingType
	 *                sorting type of this field
	 */
	public OrderingItem(Field field, OrderingType orderingType) {
		if (field == null) {
			throw new IllegalArgumentException("Ordering item field cannot be null.");
		}
		this.field = field;
		this.orderingType = orderingType;
	}
	
	/**
	 * Getter for this item's field.
	 * 
	 * @return this item's field
	 */
	public Field getField() {
		return this.field;
	}
	
	/**
	 * Getter for the sorting type of this field.
	 * 
	 * @return this field's sorting type
	 */
	public OrderingType getOrderingType() {
		return this.orderingType;
	}
	
	@Override
	public String toString() {
		return this.field.toAliasedString() + (this.orderingType == null ? "" : " " + this.orderingType);
	}
	
	/**
	 * Retrieves the final string representation of this ORDER BY field. Final string representations
	 * are used when an aggregate query is run on the master node to merge intermediate
	 * aggregate results from the worker nodes (i.e. the second and final step of execution
	 * of this query).
	 * 
	 * @param intermediateRelation
	 *                the named relation on the master node holding intermediate results
	 *                from the worker nodes
	 * @param i
	 *                unique sequential number of this field (used to identify the intermediateRelation
	 *                field corresponding to this field)
	 * @return the final string representation of this ORDER BY field
	 */
	public String toFinalString(NamedRelation intermediateRelation, int i) {
		return this.field.toAliasedFinalString(intermediateRelation, i) + (this.orderingType == null ? "" : " " + this.orderingType);
	}
}
