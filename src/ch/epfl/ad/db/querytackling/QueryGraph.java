package ch.epfl.ad.db.querytackling;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import ch.epfl.ad.db.parsing.NamedField;
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
	
	public QueryGraph(QueryRelation query) {
		if (query == null) {
			throw new IllegalArgumentException("Query graph query cannot be null.");
		}
		this.query = query;
		Map<Relation, QueryVertex> relationVertexMap = new HashMap<Relation, QueryVertex>();
		this.vertices = this.extractVertices(query, relationVertexMap);
		//this.vertices = new HashSet<QueryVertex>(relationVertexMap.values());
		this.edges = new HashMap<QueryVertex, List<QueryEdge>>();
		this.buildEdges(query, relationVertexMap);
	}
	
	public QueryGraph(QueryGraph graph) {
		if (graph == null) {
			throw new IllegalArgumentException("QueryGraph copy constructor cannot be passed null.");
		}
		this.query = graph.query;
		this.vertices = new HashSet<QueryVertex>();
		Map<QueryVertex, QueryVertex> oldVertexNewVertexMap = new HashMap<QueryVertex, QueryVertex>();
		for (QueryVertex vertex : graph.getVertices()) {
			QueryVertex vertexCopy;
			if (vertex instanceof PhysicalQueryVertex) {
				vertexCopy = vertex;
			} else {
				vertexCopy = new SuperQueryVertex(
						((SuperQueryVertex)vertex).getQuery(),
						this.extractVertices((SuperQueryVertex)vertex, oldVertexNewVertexMap),
						((SuperQueryVertex)vertex).getAlias()
						);
			}
			this.vertices.add(vertexCopy);
			oldVertexNewVertexMap.put(vertex, vertexCopy);
		}
		this.edges = new HashMap<QueryVertex, List<QueryEdge>>();
		this.buildEdges(graph, oldVertexNewVertexMap);
	}
	
	public QueryRelation getQuery() {
		return this.query;
	}
	
	public Set<QueryVertex> getVertices() {
		return this.vertices;
	}
	
	public Set<PhysicalQueryVertex> getPhysicalVerticesRecursively() {
		Set<PhysicalQueryVertex> s = new HashSet<PhysicalQueryVertex>();
		extractPhysicalVerticesRecursively(this.getVertices(), s);
		return s;
	}
	
	private void extractPhysicalVerticesRecursively(Set<QueryVertex> sv, Set<PhysicalQueryVertex> out) {
		for(QueryVertex v : sv) {
			if(v instanceof PhysicalQueryVertex)
				out.add((PhysicalQueryVertex) v);
			else
				extractPhysicalVerticesRecursively(((SuperQueryVertex) v).getVertices(), out);
		}
	}
	
	public Map<QueryVertex, List<QueryEdge>> getEdges() {
		return this.edges;
	}
	
	public List<QueryEdge> getVertexEdges(QueryVertex vertex) {
		return this.edges.get(vertex);
	}
	
	public List<QueryEdge> inheritVertex(QueryVertex heir, QueryVertex donor) {
		List<QueryEdge> newEdges = new LinkedList<QueryEdge>();
		if(this.edges.get(donor) != null) {
			if(this.edges.get(heir) == null)
				this.edges.put(heir, new LinkedList<QueryEdge>());
			for(QueryEdge edge : this.edges.get(donor)) {
				QueryEdge forward = new QueryEdge(heir, edge.getEndPoint(), edge.getJoinCondition());
				newEdges.add(forward);
				this.edges.get(heir).add(forward);
				List<QueryEdge> remoteAdj = this.edges.get(edge.getEndPoint());
				QueryEdge reverse = null;
				for(QueryEdge e : remoteAdj) {
					if(e.getEndPoint().equals(donor))
						reverse = e;
				}
				remoteAdj.remove(reverse);
				QueryEdge backward = new QueryEdge(edge.getEndPoint(), heir, reverse.getJoinCondition());
				newEdges.add(backward);
				remoteAdj.add(backward);
			}
			this.edges.remove(donor);
		}
		return newEdges;
	}
	
	public void removeEdge(QueryVertex v1, QueryVertex v2) {
		List<QueryEdge> adj1 = this.edges.get(v1);
		List<QueryEdge> adj2 = this.edges.get(v2);
		QueryEdge edge = null;
		for(QueryEdge e : adj1) {
			if(e.getEndPoint().equals(v2))
				edge = e;
		}
		adj1.remove(edge);
		if(adj1.size() == 0)
			this.edges.remove(v1);
		for(QueryEdge e : adj2) {
			if(e.getEndPoint().equals(v1))
				edge = e;
		}
		adj2.remove(edge);
		if(adj2.size() == 0)
			this.edges.remove(v2);
	}
	
	public boolean removeVertex(QueryVertex v) {
		return removeVertex(v, this.vertices);
	}
	
	private boolean removeVertex(QueryVertex v, Set<QueryVertex> sv) {
		if(sv.remove(v)) {
			if(v instanceof SuperQueryVertex)
				sv.addAll(((SuperQueryVertex) v).getVertices());
			return true;
		}
		for(QueryVertex qv : sv) {
			if(qv instanceof SuperQueryVertex)
				if(removeVertex(v, ((SuperQueryVertex) qv).getVertices()))
					return true;
		}
		return false;
	}
	
	@Override
	public String toString() {
		StringBuilder string = new StringBuilder();
		string.append("Vertices:\n");
		if (this.getVertices() != null) {
			for (QueryVertex vertex : this.getVertices()) {
				string.append(vertex).append("\n");
			}
		}
		string.append("Edges:\n");
		if (this.getEdges() != null) {
			for (List<QueryEdge> edgeList : this.getEdges().values()) {
				for (QueryEdge edge : edgeList) {
					string.append(edge).append("\n");
				}
			}
		}
		return string.toString();
	}
	
	// QueryTackler algorithm: vertices
	private Set<QueryVertex> extractVertices(Relation query, Map<Relation, QueryVertex> relationVertexMap) {
		Set<QueryVertex> vertices = new HashSet<QueryVertex>();
		if (query instanceof NamedRelation) {
			if (relationVertexMap.get(query) == null) {
				QueryVertex vertex = new PhysicalQueryVertex(((NamedRelation)query));
				vertices.add(vertex);
				relationVertexMap.put(query, vertex);
			}
		} else {
			if (((QueryRelation)query).getRelations() != null) {
				for (Relation relation : ((QueryRelation)query).getRelations()) {
					vertices.addAll(this.extractVertices(relation, relationVertexMap));
				}
			}
			if (((QueryRelation)query).getQualifiers() != null) {
				for (Qualifier qualifier : ((QueryRelation)query).getQualifiers()) {
					for (Operand operand : qualifier.getOperands()) {
						if (operand instanceof Relation) {
							vertices.addAll(this.extractVertices((Relation)operand, relationVertexMap));
						}
					}
				}
			}
		}
		if (query instanceof QueryRelation && ((QueryRelation)query).isAggregate()) {
			//if (relationVertexMap.get(query) == null || query instanceof NamedRelation) {
				Set<QueryVertex> childVertices = vertices;
				QueryVertex vertex = new SuperQueryVertex(query, childVertices, null); // unaliased
				vertices = new HashSet<QueryVertex>(1);
				vertices.add(vertex);
				relationVertexMap.put(query, vertex);
			//}
		}
		if (query instanceof QueryRelation && ((QueryRelation)query).getAlias() != null) {
			//if (relationVertexMap.get(query) == null || query instanceof NamedRelation) {
				Set<QueryVertex> childVertices = vertices;
				QueryVertex vertex = new SuperQueryVertex(query, childVertices);
				vertices = new HashSet<QueryVertex>(1);
				vertices.add(vertex);
				relationVertexMap.put(query, vertex);
			//}
		}
		return vertices;
	}
	
	// QueryTackler algorithm: edges (vertices must already exist)
	private void buildEdges(Relation query, Map<Relation, QueryVertex> relationVertexMap) {
		if (query instanceof NamedRelation) {
			return;
		} else {
			if (((QueryRelation)query).getRelations() != null) {
				for (Relation relation : ((QueryRelation)query).getRelations()) {
					this.buildEdges(relation, relationVertexMap);
				}
			}
			if (((QueryRelation)query).getQualifiers() != null) {
				for (Qualifier qualifier : ((QueryRelation)query).getQualifiers()) {
					for (Operand operand : qualifier.getOperands()) {
						if (operand instanceof Relation) {
							this.buildEdges((Relation)operand, relationVertexMap);
						}
					}
						
					// Edges == equi-join conditions...
					if (qualifier.getOperator() == Operator.EQUALS) {
						List<Operand> operands = qualifier.getOperands();
						
						// ...where both operands are field references...
						if (operands.get(0) instanceof NamedField && operands.get(1) instanceof NamedField) {
							NamedField field1 = (NamedField)operands.get(0);
							NamedField field2 = (NamedField)operands.get(1);
							Relation relation1 = field1.getRelation();
							Relation relation2 = field2.getRelation();
							
							// ...pointing to two different tables
							if (!relation1.equals(relation2)) {
								QueryVertex vertex1 = relationVertexMap.get(relation1);
								QueryVertex vertex2 = relationVertexMap.get(relation2);
								
								// ~Add vertex1 -> vertex2 edge if relation1 is in RelationList of query
								//if (((QueryRelation)query).getRelations().contains(relation1)) {
									if (this.edges.get(vertex1) == null) {
										this.edges.put(vertex1, new LinkedList<QueryEdge>());
									}
									this.edges.get(vertex1).add(new QueryEdge(
											vertex1,
											vertex2,
											new JoinCondition(field1.getField(), field2.getField())
											));
								//}
								
								// ~Add vertex2 -> vertex1 edge if relation2 is in RelationList of query
								//if (((QueryRelation)query).getRelations().contains(relation2)) {
									if (this.edges.get(vertex2) == null) {
										this.edges.put(vertex2, new LinkedList<QueryEdge>());
									}
									this.edges.get(vertex2).add(new QueryEdge(
											vertex2,
											vertex1,
											new JoinCondition(field2.getField(), field1.getField())
											));
										
								//}
							}
						}
					}
				}
			}
		}
	}
	
	// Copy constructor: vertices
	private Set<QueryVertex> extractVertices(SuperQueryVertex superQueryVertex, Map<QueryVertex, QueryVertex> oldVertexNewVertexMap) {
		Set<QueryVertex> vertices = new HashSet<QueryVertex>();
		for (QueryVertex vertex : superQueryVertex.getVertices()) {
			QueryVertex vertexCopy;
			if (vertex instanceof PhysicalQueryVertex) {
				vertexCopy = vertex;
			} else {
				vertexCopy = new SuperQueryVertex(
						((SuperQueryVertex)vertex).getQuery(),
						this.extractVertices((SuperQueryVertex)vertex, oldVertexNewVertexMap)
						);
			}
			vertices.add(vertexCopy);
			oldVertexNewVertexMap.put(vertex, vertexCopy);
		}
		return vertices;
	}
	
	// Copy constructor: edges
	private void buildEdges(QueryGraph graph, Map<QueryVertex, QueryVertex> oldVertexNewVertexMap) {
		for (Entry<QueryVertex, List<QueryEdge>> vertexEdges : graph.getEdges().entrySet()) {
			QueryVertex newVertex = oldVertexNewVertexMap.get(vertexEdges.getKey());
			List<QueryEdge> edgeListCopy = new LinkedList<QueryEdge>();
			for (QueryEdge edge : vertexEdges.getValue()) {
				edgeListCopy.add(new QueryEdge(
						newVertex,
						oldVertexNewVertexMap.get(edge.getEndPoint()),
						edge.getJoinCondition()
						));
			}
			this.edges.put(newVertex, edgeListCopy);
		}
	}
}
