package ch.epfl.ad.db.querytackling;

/**
 * An edge of an SQL query graph.
 * 
 * @author Artyom Stetsenko
 */
public class QueryEdge {
	
	/**
	 * The edge's start vertex.
	 */
	private QueryVertex startPoint;
	
	/**
	 * The edge's end vertex. 
	 */
	private QueryVertex endPoint;
	
	/**
	 * The equijoin condition represented by this edge. 
	 */
	private JoinCondition joinCondition;
	
	/**
	 * Constructor of query edge.
	 * 
	 * @param startPoint
	 *                edge's start vertex
	 * @param endPoint
	 *                edge's end vertex
	 * @param joinCondition
	 *                edge's join condition
	 */
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
	
	/**
	 * Retrieves this edge's start vertex.
	 * 
	 * @return this edge's start vertex
	 */
	public QueryVertex getStartPoint() {
		return this.startPoint;
	}
	
	/**
	 * Retrieves this edge's end vertex.
	 * 
	 * @return this edge's end vertex
	 */
	public QueryVertex getEndPoint() {
		return this.endPoint;
	}
	
	/**
	 * Retrieves the equijoin condition represented by this edge.
	 * 
	 * @return the equijoin condition between start vertex and end vertex
	 */
	public JoinCondition getJoinCondition() {
		return this.joinCondition;
	}
	
	@Override
	public String toString() {
		return this.startPoint + "." + this.joinCondition.getStartPointField() +
				" -> " + this.endPoint + "." + this.joinCondition.getEndPointField();
	}
}
