package ch.epfl.ad.db.querytackling;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;
import java.util.Set;

import ch.epfl.ad.db.queryexec.ExecStep;
import ch.epfl.ad.db.queryexec.StepAggregate;
import ch.epfl.ad.db.queryexec.StepDistribute;
import ch.epfl.ad.db.queryexec.StepGather;
import ch.epfl.ad.db.queryexec.StepJoin;
import ch.epfl.ad.db.queryexec.StepSuperDuper;
import ch.epfl.ad.db.queryexec.ExecStep.StepPlace;

public class GraphProcessor {
	
	public static class QueryNotSupportedException extends Exception {
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
	List<ExecStep> execSteps;
	
	public GraphProcessor(QueryGraph g) {
		graph = new QueryGraph(g);
		edges = graph.getEdges();
		execSteps = new LinkedList<ExecStep>();
		try {
			if(graph.getVertices().size() == 1 &&
					graph.getVertices().iterator().next() instanceof SuperQueryVertex &&
					((SuperQueryVertex) graph.getVertices().iterator().next()).isAggregate()) {
				// if the graph is one big aggregate bubble, then process it directly
				process(graph.getVertices().iterator().next());
			} else {
				// otherwise, put the whole thing in a single big bubble
				process(new SuperQueryVertex(graph.getQuery(), graph.getVertices(), "whole_query"));
			}
		} catch (QueryNotSupportedException e) {
			System.out.println("Oh! Ow, this query is not supported :/");
		}
		System.out.println(Arrays.toString(execSteps.toArray()));
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
		// if we only have 1, we return
		if(sqv.getVertices().size() == 1) {
			PhysicalQueryVertex singleVertex = (PhysicalQueryVertex) sqv.getVertices().iterator().next();
			if(!sqv.isAggregate()) {
				if(!sqv.getAlias().equals("whole_query"))
					return singleVertex;
				// Top level bubble, need to gather
				NDQueryVertex newVertex = new NDQueryVertex(singleVertex.getName() + "_" + new Random().nextInt(1000));
				execSteps.add(new StepGather(singleVertex.getName(), newVertex.getName()));
				return newVertex;
			}
			NDQueryVertex newVertex = new NDQueryVertex(singleVertex.getName() + "_" + new Random().nextInt(1000));
			String oldVertexName = singleVertex.getName();
			if(!(singleVertex instanceof NDQueryVertex)) { // if distributed
				oldVertexName += "_" + new Random().nextInt(1000);
				execSteps.add(new StepAggregate("TODO " + singleVertex.getName(), oldVertexName, StepPlace.ON_WORKERS));
				//System.out.println("Aggregate everywhere " + singleVertex.getName() + " INTO " + oldVertexName + " on master");
			}
			execSteps.add(new StepAggregate("TODO " + oldVertexName, newVertex.getName(), StepPlace.ON_MASTER));
			//System.out.println("Aggregate on master " + oldVertexName + " INTO " + newVertex.getName());
			return newVertex;
		}
		StringBuilder joinedTblName = new StringBuilder();
		List<String> toCrossJoin = new ArrayList<String>();
		for(QueryVertex qv : sqv.getVertices()){
			if(qv instanceof NDQueryVertex) {
				toCrossJoin.add(((PhysicalQueryVertex) qv).getName());
			} else {
				String tempTblName = ((PhysicalQueryVertex) qv).getName() + "_" + new Random().nextInt(1000);
				execSteps.add(new StepGather(((PhysicalQueryVertex) qv).getName(), tempTblName));
				/*System.out.println("Gather on master " + ((PhysicalQueryVertex) qv).getName() +
						" INTO " + tempTblName);*/
				toCrossJoin.add(tempTblName);
			}
			joinedTblName.append(((PhysicalQueryVertex) qv).getName() + "_");
		}
		joinedTblName.append(new Random().nextInt(1000));
		execSteps.add(new StepJoin(toCrossJoin, null, joinedTblName.toString(), StepPlace.ON_MASTER));
		/*System.out.println("Join on master " + Arrays.toString(toCrossJoin.toArray()) +
				" INTO " + joinedTblName.toString());*/
		if(sqv.isAggregate()) {
			String oldName = joinedTblName.toString();
			joinedTblName.append("_" + new Random().nextInt(1000));
			execSteps.add(new StepAggregate("TODO " + oldName, joinedTblName.toString(), StepPlace.ON_MASTER));
			//System.out.println("Aggregate on master " + oldName + " INTO " + joinedTblName.toString());
		}
		return new NDQueryVertex(joinedTblName.toString());
		
		
		/*boolean containsD = false;
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
		return ndqv;*/
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
		List<String> toJoinStr = new ArrayList<String>();
		List<String> joinCondStr = new ArrayList<String>();
		PhysicalQueryVertex isp = (PhysicalQueryVertex) edges.get(pqv).get(0).getStartPoint();
		toJoinStr.add(isp.getName());
		Map<QueryEdge, PhysicalQueryVertex> history = new HashMap<QueryEdge, PhysicalQueryVertex>();
		while(edges.get(pqv) != null) {
			QueryEdge edge = edges.get(pqv).get(0);
			PhysicalQueryVertex sp = (PhysicalQueryVertex) edge.getStartPoint();
			PhysicalQueryVertex ep = (PhysicalQueryVertex) edge.getEndPoint();
			PhysicalQueryVertex tempEPTbl = new PhysicalQueryVertex(ep.getName() + "_" + new Random().nextInt(1000));
			toJoinStr.add(tempEPTbl.getName());
			joinCondStr.add(edge.getJoinCondition().toString());
			PhysicalQueryVertex joined = new PhysicalQueryVertex(sp.getName() + "_" + tempEPTbl.getName());
			PhysicalQueryVertex shipTo = sp;
			if(history.get(edge) != null)
				shipTo = history.get(edge);
			if(ep instanceof NDQueryVertex) {
				execSteps.add(new StepDistribute(ep.getName(), shipTo.getName(), edge.getJoinCondition().getEndPointField(), edge.getJoinCondition().getStartPointField(), tempEPTbl.getName()));
				/*System.out.println("Distribute (using bloom) " + ep.getName() +
						" TO " + shipTo.getName() +  
						" ON " + edge.getJoinCondition() +
						" INTO " + tempEPTbl.getName());*/
			} else {
				execSteps.add(new StepSuperDuper(ep.getName(), shipTo.getName(), edge.getJoinCondition().getEndPointField(), edge.getJoinCondition().getStartPointField(), tempEPTbl.getName()));
				/*System.out.println("SuperDuper " + ep.getName() +
						" TO " + shipTo.getName() +  
						" ON " + edge.getJoinCondition() +
						" INTO " + tempEPTbl.getName());*/
			}
			graph.removeEdge(sp, ep);
			for(QueryEdge e : graph.inheritVertex(joined, ep)) {
				history.put(e, tempEPTbl);
			}
			for(QueryEdge e : graph.inheritVertex(joined, sp)) {
				history.put(e, isp);
			}
			vertices.remove(sp);
			vertices.remove(ep);
			vertices.add(joined);
			pqv = joined;
		}
		execSteps.add(new StepJoin(toJoinStr, joinCondStr, pqv.getName(), StepPlace.ON_WORKERS));
		/*System.out.println("Join everywhere " + Arrays.toString(toJoinStr.toArray()) +
				" ON " + Arrays.toString(joinCondStr.toArray()) +
				" INTO " + pqv.getName());*/
	}
	
	private void eatAllEdgesND(Set<QueryVertex> vertices, PhysicalQueryVertex pqv) { // both are ND
		List<String> toJoinStr = new ArrayList<String>();
		List<String> joinCondStr = new ArrayList<String>();
		toJoinStr.add(((PhysicalQueryVertex) edges.get(pqv).get(0).getStartPoint()).getName());
		while(edges.get(pqv) != null) {
			QueryEdge edge = edges.get(pqv).get(0);
			PhysicalQueryVertex sp = (PhysicalQueryVertex) edge.getStartPoint();
			PhysicalQueryVertex ep = (PhysicalQueryVertex) edge.getEndPoint();
			toJoinStr.add(ep.getName());
			joinCondStr.add(edge.getJoinCondition().toString());
			PhysicalQueryVertex joined = new NDQueryVertex(sp.getName() +
					"_" + ep.getName() +
					"_" + new Random().nextInt(1000));
			graph.removeEdge(sp, ep);
			graph.inheritVertex(joined, ep);
			graph.inheritVertex(joined, sp);
			vertices.remove(sp);
			vertices.remove(ep);
			vertices.add(joined);
			pqv = joined;
		}
		execSteps.add(new StepJoin(toJoinStr, joinCondStr, pqv.getName(), StepPlace.ON_MASTER));
		/*System.out.println("Join on master " + Arrays.toString(toJoinStr.toArray()) +
				" ON " + Arrays.toString(joinCondStr.toArray()) +
				" INTO " + pqv.getName());*/
	}

}
