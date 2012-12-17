package ch.epfl.ad.db.parsing;

public enum OrderingType {
	
	ASC,
	DESC;
	
	private static final OrderingType[] allValues = OrderingType.values();
	
	public static OrderingType forTypeName(String typeName) {
		for (OrderingType orderingType : OrderingType.allValues) {
            if (orderingType.toString().equalsIgnoreCase(typeName)) {
            	return orderingType;
            }
        }
		return null;
	}
}
