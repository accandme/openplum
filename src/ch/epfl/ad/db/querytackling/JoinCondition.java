package ch.epfl.ad.db.querytackling;

public class JoinCondition {
	
	private String startPointField;
	private String endPointField;
	
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

	public String getStartPointField() {
		return this.startPointField;
	}
	
	public String getEndPointField() {
		return this.endPointField;
	}
	
	public String toString() {
		return this.startPointField + " = " + this.endPointField;
	}
	
}
