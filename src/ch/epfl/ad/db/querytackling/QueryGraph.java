package ch.epfl.ad.db.querytackling;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import ch.epfl.ad.db.parsing.Field;
import ch.epfl.ad.db.parsing.NamedRelation;
import ch.epfl.ad.db.parsing.Operand;
import ch.epfl.ad.db.parsing.Operator;
import ch.epfl.ad.db.parsing.Qualifier;
import ch.epfl.ad.db.parsing.QueryRelation;
import ch.epfl.ad.db.parsing.Relation;

public class QueryGraph {
	
	private QueryRelation query;
	private Set<QueryVertex> vertices;
	private Map<QueryVertex, List<QueryEdge>> edges;
	private Map<Relation, QueryVertex> relationVertexMap;
	
	public QueryGraph(QueryRelation query) {
		if (query == null) {
			throw new IllegalArgumentException("Query graph query cannot be null.");
		}
		this.query = query;
		this.extractVertices(query);
		this.vertices = new HashSet<QueryVertex>();
		for (QueryVertex vertex : this.relationVertexMap.values()) {
			if (vertex instanceof PhysicalQueryVertex) {
				this.vertices.add(vertex);
			}
		}
		this.buildEdges(query);
	}
	
	public QueryRelation getQuery() {
		return this.query;
	}
	
	public Set<QueryVertex> getVertices() {
		return this.vertices;
	}
	
	public Map<QueryVertex, List<QueryEdge>> getEdges() {
		return this.edges;
	}
	
	public List<QueryEdge> getVertexEdges(QueryVertex vertex) {
		return this.edges.get(vertex);
	}
	
	@Override
	public String toString() {
		StringBuilder string = new StringBuilder();
		string.append("Vertices:\n");
		if (this.vertices != null) {
			for (QueryVertex vertex : this.getVertices()) {
				string.append(vertex).append("\n");
			}
		}
		string.append("Edges:\n");
		if (this.relationVertexMap != null) {
			for (QueryVertex vertex : this.relationVertexMap.values()) {
				if (this.getVertexEdges(vertex) != null) {
					for (QueryEdge edge : this.getVertexEdges(vertex)) {
						string.append(edge).append("\n");
					}
				}
			}
		}
		return string.toString();
	}
	
	// QueryTackler algorithm: vertices
	private Set<QueryVertex> extractVertices(Relation query) {
		if (this.relationVertexMap == null) {
			this.relationVertexMap = new HashMap<Relation, QueryVertex>();
		}
		Set<QueryVertex> vertices = new HashSet<QueryVertex>();
		if (query instanceof NamedRelation) {
			if (this.relationVertexMap.get(query) == null) {
				QueryVertex vertex = new PhysicalQueryVertex(((NamedRelation)query).getName());
				vertices.add(vertex);
				this.relationVertexMap.put(query, vertex);
			}
		} else {
			if (((QueryRelation)query).getRelations() != null) {
				for (Relation relation : ((QueryRelation)query).getRelations()) {
					vertices.addAll(this.extractVertices(relation));
				}
			}
			if (((QueryRelation)query).getQualifiers() != null) {
				for (Qualifier qualifier : ((QueryRelation)query).getQualifiers()) {
					for (Operand operand : qualifier.getOperands()) {
						if (operand instanceof Relation) {
							vertices.addAll(this.extractVertices((Relation)operand));
						}
					}
				}
			}
		}
		if (query.getAlias() != null) {
			if (this.relationVertexMap.get(query) == null) {
				Set<QueryVertex> childVertices = vertices;
				QueryVertex vertex = new SuperQueryVertex(query.getAlias(), childVertices);
				vertices = new HashSet<QueryVertex>(1);
				vertices.add(vertex);
				this.relationVertexMap.put(query, vertex);
			}
		}
		return vertices;
	}
	
	// QueryTackler algorithm: edges (vertices must already exist)
	private void buildEdges(Relation query) {
		if (this.edges == null) {
			this.edges = new HashMap<QueryVertex, List<QueryEdge>>();
		}
		if (query instanceof NamedRelation) {
			return;
		} else {
			if (((QueryRelation)query).getRelations() != null) {
				for (Relation relation : ((QueryRelation)query).getRelations()) {
					this.buildEdges(relation);
				}
			}
			if (((QueryRelation)query).getQualifiers() != null) {
				for (Qualifier qualifier : ((QueryRelation)query).getQualifiers()) {
					for (Operand operand : qualifier.getOperands()) {
						if (operand instanceof Relation) {
							this.buildEdges((Relation)operand);
						}
					}
						
					// Edges == equi-join conditions...
					if (qualifier.getOperator() == Operator.EQUALS) {
						List<Operand> operands = qualifier.getOperands();
						
						// ...where both operands are field references...
						if (operands.get(0) instanceof Field && operands.get(1) instanceof Field) {
							Field field1 = (Field)operands.get(0);
							Field field2 = (Field)operands.get(1);
							Relation relation1 = field1.getRelation();
							Relation relation2 = field2.getRelation();
							
							// ...pointing to two different tables
							if (!relation1.equals(relation2)) {
								QueryVertex vertex1 = this.relationVertexMap.get(relation1);
								QueryVertex vertex2 = this.relationVertexMap.get(relation2);
								
								// Add vertex1 -> vertex2 edge if relation1 is in RelationList of query
								if (((QueryRelation)query).getRelations().contains(relation1)) {
									if (this.edges.get(vertex1) == null) {
										this.edges.put(vertex1, new LinkedList<QueryEdge>());
									}
									this.edges.get(vertex1).add(new QueryEdge(
											vertex1,
											vertex2,
											new JoinCondition(field1.getField(), field2.getField())
											));
								}
								
								// Add vertex2 -> vertex1 edge if relation2 is in RelationList of query
								if (((QueryRelation)query).getRelations().contains(relation2)) {
									if (this.edges.get(vertex2) == null) {
											this.edges.put(vertex2, new LinkedList<QueryEdge>());
										}
										this.edges.get(vertex2).add(new QueryEdge(
												vertex2,
												vertex1,
												new JoinCondition(field2.getField(), field1.getField())
												));
										
								}
							}
						}
					}
				}
			}
		}
	}
}
