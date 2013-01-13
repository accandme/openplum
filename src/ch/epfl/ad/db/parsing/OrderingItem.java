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
		return this.field.toAliasedString() + (this.orderingType == null ? "" : " " + this.orderingType);
	}
	
	public String toFinalString(NamedRelation intermediateRelation, int i) {
		return this.field.toAliasedFinalString(intermediateRelation, i) + (this.orderingType == null ? "" : " " + this.orderingType);
	}
}
