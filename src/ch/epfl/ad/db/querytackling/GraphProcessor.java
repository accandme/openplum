package ch.epfl.ad.db.querytackling;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class GraphProcessor {
	
	public class QueryNotSupportedException extends Exception {
		public QueryNotSupportedException(String string) {
			super(string);
		}
		private static final long serialVersionUID = 9133124936966073467L;
	}
	
	class NDQueryVertex extends PhysicalQueryVertex {
		public NDQueryVertex(String name) {
			super(name);
		}
	}
	
	QueryGraph graph;
	Map<QueryVertex, List<QueryEdge>> edges;
	
	public GraphProcessor(QueryGraph g) {
		graph = new QueryGraph(g);
		edges = graph.getEdges();
		try {
			process(new SuperQueryVertex(graph.getQuery(), graph.getVertices(), "whole_query"));
		} catch (QueryNotSupportedException e) {
			System.out.println("Oh! Ow, this query is not supported :/");
		}
	}

	public QueryVertex process(QueryVertex sqv1) throws QueryNotSupportedException {
		// if it is not a super node, no need to do anything
		if(!(sqv1 instanceof SuperQueryVertex))
			return sqv1;
		// check if query is valid (do not allow edges crossing boundaries)
		SuperQueryVertex sqv = (SuperQueryVertex) sqv1;
		for(QueryVertex qv : sqv.getVertices()){
			if(edges.get(qv) != null)
				for(QueryEdge qe : edges.get(qv))
					if(!sqv.getVertices().contains(qe.getEndPoint()))
						throw new QueryNotSupportedException("Edge Crossing Bubble Boundary");
		}
		// get rid of all super nodes in the current level
		Set<QueryVertex> toAdd = new HashSet<QueryVertex>();
		for(Iterator<QueryVertex> it = sqv.getVertices().iterator(); it.hasNext(); ) {
			QueryVertex qv = it.next();
			if(qv instanceof SuperQueryVertex) {
				PhysicalQueryVertex pqv = (PhysicalQueryVertex) process(qv);
				graph.inheritVertex(pqv, qv);
				it.remove();
				toAdd.add(pqv);
			}
		}
		sqv.getVertices().addAll(toAdd);
		// now that we dont have super nodes, do the logic
		PhysicalQueryVertex picked;
		while((picked = pickConnectedPhysical(sqv.getVertices())) != null) {
			eatAllEdgesPhysical(sqv.getVertices(), picked);
			//System.out.println("####### NEW STATE");
			//System.out.println(graph);
		}
		while((picked = pickConnectedND(sqv.getVertices())) != null) {
			eatAllEdgesND(sqv.getVertices(), picked);
			//System.out.println("####### NEW STATE");
			//System.out.println(graph);
		}
		// now we have disconnected physical tables and NDs
		boolean containsD = false;
		for(QueryVertex qv : sqv.getVertices()){
			if(!(qv instanceof NDQueryVertex)) {
				containsD = true;
				break;
			}
		}
		String tableName = "_" + sqv.getAlias();
		boolean skip = true;
		for(QueryVertex qv : sqv.getVertices()){
			if(qv instanceof NDQueryVertex) {
				if(containsD)
					System.out.println("Replicate " + ((NDQueryVertex) qv).getName());
			} else {
				if(!skip)
					System.out.println("Gather and Replicate " + ((PhysicalQueryVertex) qv).getName());
				skip = false;
			}
			tableName += "_" + ((PhysicalQueryVertex) qv).getName();
		}
		if(containsD)
			System.out.println("Run sub query (whose alias is " + sqv.getAlias() + "), and send results to " + tableName + " on master");
		else
			System.out.println("Run sub query (whose alias is " + sqv.getAlias() + ") on master, and you're done!");
		sqv.getVertices().clear();
		NDQueryVertex ndqv = new NDQueryVertex(tableName);
		sqv.getVertices().add(ndqv);
		return ndqv;
	}
	
	private PhysicalQueryVertex pickConnectedPhysical(Set<QueryVertex> vertices) {
		// TODO pick smartly (e.g. choose largest table)
		for(QueryVertex qv : vertices)
			if(!(qv instanceof NDQueryVertex))
				if(edges.get(qv) != null)
					return (PhysicalQueryVertex) qv;
		return null;
	}
	
	private PhysicalQueryVertex pickConnectedND(Set<QueryVertex> vertices) {
		// TODO pick smartly (e.g. choose largest table)
		for(QueryVertex qv : vertices)
			if(edges.get(qv) != null)
				return (PhysicalQueryVertex) qv;
		return null;
	}
	
	private void eatAllEdgesPhysical(Set<QueryVertex> vertices, PhysicalQueryVertex pqv) {
		while(edges.get(pqv) != null) {
			QueryEdge edge = edges.get(pqv).get(0);
			PhysicalQueryVertex sp = (PhysicalQueryVertex) edge.getStartPoint();
			PhysicalQueryVertex ep = (PhysicalQueryVertex) edge.getEndPoint();
			PhysicalQueryVertex joined = new PhysicalQueryVertex("_" + sp.getName() + "_" + ep.getName());
			if(ep instanceof NDQueryVertex)
				System.out.println("Distribute/Bloom & join " + ep.getName() + " to " + sp.getName() + " on " + edge.getJoinCondition() + ", result in " + joined.getName());
			else
				System.out.println("SuperDuper & join " + ep.getName() + " to " + sp.getName() + " on " + edge.getJoinCondition() + ", result in " + joined.getName());
			graph.removeEdge(sp, ep);
			graph.inheritVertex(joined, ep);
			graph.inheritVertex(joined, sp);
			//graph.removeVertex(ep);
			vertices.remove(sp);
			vertices.remove(ep);
			vertices.add(joined);
			pqv = joined;
		}
	}
	
	private void eatAllEdgesND(Set<QueryVertex> vertices, PhysicalQueryVertex pqv) { // both are ND
		while(edges.get(pqv) != null) {
			QueryEdge edge = edges.get(pqv).get(0);
			PhysicalQueryVertex sp = (PhysicalQueryVertex) edge.getStartPoint();
			PhysicalQueryVertex ep = (PhysicalQueryVertex) edge.getEndPoint();
			PhysicalQueryVertex joined = new PhysicalQueryVertex("_" + sp.getName() + "_" + ep.getName());
			System.out.println("Join on master " + ep.getName() + " with " + sp.getName() + " on " + edge.getJoinCondition() + ", result in " + joined.getName());
			graph.removeEdge(sp, ep);
			graph.inheritVertex(joined, ep);
			graph.inheritVertex(joined, sp);
			//graph.removeVertex(ep);
			vertices.remove(sp);
			vertices.remove(ep);
			vertices.add(joined);
			pqv = joined;
		}
	}

}
