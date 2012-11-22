package ch.epfl.ad.db.querytackling;

public class QueryEdge {
	
	private QueryVertex startPoint;
	private QueryVertex endPoint;
	private JoinCondition joinCondition;
	
	public QueryEdge(QueryVertex startPoint, QueryVertex endPoint, JoinCondition joinCondition) {
		if (startPoint == null) {
			throw new IllegalArgumentException("Edge start vertex cannot be null.");
		}
		if (endPoint == null) {
			throw new IllegalArgumentException("Edge end vertex cannot be null.");
		}
		if (joinCondition == null) {
			throw new IllegalArgumentException("Edge join condition cannot be null.");
		}
		this.startPoint = startPoint;
		this.endPoint = endPoint;
		this.joinCondition = joinCondition;
	}
	
	public QueryVertex getStartPoint() {
		return this.startPoint;
	}
	
	public QueryVertex getEndPoint() {
		return this.endPoint;
	}
	
	public JoinCondition getJoinCondition() {
		return this.joinCondition;
	}
	
	@Override
	public String toString() {
		return this.startPoint + "." + this.joinCondition.getStartPointField() +
				" -> " + this.endPoint + "." + this.joinCondition.getEndPointField();
	}
}
