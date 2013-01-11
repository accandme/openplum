package ch.epfl.ad.db.parsing;

public class OrderingItem {
	
	private Field field;
	private OrderingType orderingType;
	
	public OrderingItem(Field field) {
		this(field, null);
	}
	
	public OrderingItem(Field field, OrderingType orderingType) {
		if (field == null) {
			throw new IllegalArgumentException("Ordering item field cannot be null.");
		}
		this.field = field;
		this.orderingType = orderingType;
	}
	
	public Field getField() {
		return this.field;
	}
	
	public OrderingType getOrderingType() {
		return this.orderingType;
	}
	
	@Override
	public String toString() {
		return this.toString(QueryType.REGULAR);
	}
	
	public String toString(QueryType type) {
		return this.orderingType == null ? this.field.toAliasedString(type) : this.field.toAliasedString(type) + " " + this.orderingType;
	}
}
