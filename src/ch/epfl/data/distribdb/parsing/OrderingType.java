package ch.epfl.data.distribdb.parsing;

/**
 * Enum representing a sorting type for a field in SQL ORDER BY clause (ASC or DESC).
 * 
 * @author Artyom Stetsenko
 */
public enum OrderingType {
	
	ASC,
	DESC;
	
	private static final OrderingType[] allValues = OrderingType.values();
	
	/**
	 * Retrieves the enum for the specified sorting type.
	 * 
	 * @param typeName
	 *                  sorting type to return the enum for
	 * @return the enum representing typeName
	 */
	public static OrderingType forTypeName(String typeName) {
		for (OrderingType orderingType : OrderingType.allValues) {
            if (orderingType.toString().equalsIgnoreCase(typeName)) {
            	return orderingType;
            }
        }
		return null;
	}
}
