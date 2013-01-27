package ch.epfl.data.distribdb.tackling;

/**
 * An equijoin condition of an SQL graph edge. Each endpoint of this condition is the name of
 * a field in the corresponding vertex.
 * 
 * @author Artyom Stetsenko
 */
public class JoinCondition {
	
	/**
	 * Start (left) field of this join condition (field in start vertex).
	 */
	private String startPointField;
	
	/**
	 * End (right) field of this join condition (field in end vertex). 
	 */
	private String endPointField;
	
	/**
	 * Constructor of a join condition.
	 * 
	 * @param startPointField
	 *                start (left) field of this join condition
	 * @param endPointField
	 *                end (right) field of this join condition
	 */
	public JoinCondition(String startPointField, String endPointField) {
		if (startPointField == null) {
			throw new IllegalArgumentException("Join condition startpoint field cannot be null.");
		}
		if (endPointField == null) {
			throw new IllegalArgumentException("Join condition endpoint field cannot be null.");
		}
		this.startPointField = startPointField;
		this.endPointField = endPointField;
	}

	/**
	 * Retrieves the start (left) field of this join condition.
	 * 
	 * @return the start (left) field of this join condition
	 */
	public String getStartPointField() {
		return this.startPointField;
	}
	
	/**
	 * Retrieves the end (right) field of this join condition.
	 * 
	 * @return the end (right) field of this join condition.
	 */
	public String getEndPointField() {
		return this.endPointField;
	}
	
	public String toString() {
		return this.startPointField + " = " + this.endPointField;
	}
	
}
