package ch.epfl.data.distribdb.execution;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import ch.epfl.data.distribdb.execution.ExecStep.StepPlace;
import ch.epfl.data.distribdb.parsing.NamedRelation;
import ch.epfl.data.distribdb.parsing.QueryRelation;
import ch.epfl.data.distribdb.tackling.PhysicalQueryVertex;
import ch.epfl.data.distribdb.tackling.QueryEdge;
import ch.epfl.data.distribdb.tackling.QueryGraph;
import ch.epfl.data.distribdb.tackling.QueryVertex;
import ch.epfl.data.distribdb.tackling.SuperQueryVertex;

/**
 * GraphProcessor - The logic that comes up 
 * with a distributed query plan for a query
 * 
 * This class is responsible for finding an optimized 
 * distributed query plan for a query
 * It takes as input the query graph that was generated 
 * for this query, and processes the graph using 
 * heuristics for optimization
 * The output is a sequence of execution steps that 
 * are fed to the step executor in order to execute them
 * The logic of this class works as follows 
 * The input (which is the graph) is a SuperVertex
 * we process super vertices recursively
 * for all children super vertices we recursively 
 * transform them into physical vertices
 * then we are left with physical vertices only
 * which are either distributed (D) or 
 * non-distributed (ND) with edges 
 * between them potentially
 * While we have edges we eat them (by performing 
 * a SuperDuper step) we start by picking a D connected 
 * vertex and eat all edges connected to it (and to 
 * any vertex reachable from it), i.e., we fuse connected 
 * components in the graph
 * We keep on doing this until we cannot find any D 
 * connected vertex, then we look for an ND connected vertex
 * and do the same
 * Once we have no more connected vertices we have two options
 * Either we are left with one physical vertex (which means the 
 * graph was connected) or we have not connected vertices 
 * In the latter case we transform all vertices to 
 * ND (meaning we ship them to the master) and run the 
 * sub-query there
 * For the former case, we replace the physical vertex by 
 * another one which will contain the result of running the sub-query
 * attached to the super vertex (bubble) that we are currently processing
 * depending on whether the original physical vertex was D or ND and 
 * depending on whether the sub-query in hands is aggregate (AGG) or not
 * we decide on whether the output table (the result physical vertex)
 * should be D or ND, and in each case (all in all we have four cases 
 * D-AGG D-NAGG ND-AGG ND-NAGG) we enqueue the corresponding 
 * steps to execute
 * 
 * @author Amer C (amer.chamseddine@epfl.ch)
 *
 */
public class GraphProcessor {
	
	/**
	 * Handle to the graph to process
	 */
	QueryGraph graph;
	/**
	 * Handle to the graph edges
	 * This is just a shortcut to graph.getEdges()
	 */
	Map<QueryVertex, List<QueryEdge>> edges;
	/**
	 * List that gets populated by the elementary 
	 * steps to be executed as the distributed query 
	 * plan of the query which we are given the graph
	 */
	List<ExecStep> execSteps = new LinkedList<ExecStep>();
	/**
	 * Table Manager - responsible mainly for 
	 * generating unique names for temporary tables,
	 * keeping track of them,
	 * and deleting them at the end
	 */
	TableManager tableManager;
	
	/**
	 * Constructor - Initializes the object with the graph 
	 * of the query that we want to process, and a handle to 
	 * the table manager 
	 * The constructor clones the graph because this object 
	 * consumes it when it processes it
	 * 
	 * @param QueryGraph of the query in hands
	 */
	public GraphProcessor(TableManager tableManager, QueryGraph graph) {
		this.tableManager = tableManager;
		this.graph = new QueryGraph(graph);
		edges = this.graph.getEdges();
	}
	
	/**
	 * The interface function that can be called to process 
	 * the graph that was given at the construction time
	 * It adds a outer bubble to the graph so that 
	 * the process function can process it
	 * This function returns the generated list 
	 * of steps which represent the distributed 
	 * query plan of the query represented by 
	 * the graph given to the constructor
	 * 
	 * @return List<ExecStep> the list of execution steps 
	 * @throws QueryNotSupportedException
	 */
	public  List<ExecStep> processGraph() throws QueryNotSupportedException {
		process(new SuperQueryVertex(graph.getQuery(), graph.getVertices(), "whole_query"));
		return execSteps;
	}

	/**
	 * The main function that recursively processes super vertices 
	 * and returns the physical vertex that should replace them
	 * in the query graph
	 * The logic of this function is as described in the class description
	 * We can clearly see the 4 cases when we only have 1 physical vertex 
	 * after processing super vertices and eating all edges  
	 * 
	 * @param super vertex to be processed
	 * @return the physical vertex that should replace 
	 * the processed vertex
	 * @throws QueryNotSupportedException
	 */
	private QueryVertex process(QueryVertex sqv1) throws QueryNotSupportedException {
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
				// when we have an aggregation bubble inside a non aggregation bubble, both point to the same query!
				if(sqv.getQuery() == ((SuperQueryVertex) qv).getQuery())
					return pqv;
				((QueryRelation) sqv.getQuery()).replaceRelation(((SuperQueryVertex) qv).getQuery(), (pqv.getRelation()));
				graph.inheritVertex(pqv, qv);
				it.remove();
				toAdd.add(pqv);
			}
		}
		sqv.getVertices().addAll(toAdd);
		// now that we dont have super nodes, do the logic
		Set<QueryVertex> nodeChildren = new HashSet<QueryVertex>();
		nodeChildren.addAll(sqv.getVertices());
		List<StepSuperDuper> eatenEdgesSteps = new LinkedList<StepSuperDuper>();
		PhysicalQueryVertex picked;
		while((picked = pickConnectedPhysical(sqv.getVertices())) != null) {
			eatAllEdgesPhysical(sqv.getVertices(), picked, eatenEdgesSteps);
		}
		while((picked = pickConnectedND(sqv.getVertices())) != null) {
			eatAllEdgesND(sqv.getVertices(), picked, eatenEdgesSteps);
		}
		// now we have disconnected physical tables and NDs
		
		// if we only have 1, we return
		if(sqv.getVertices().size() == 1) {
			for(StepSuperDuper es : eatenEdgesSteps) {
				((QueryRelation) sqv.getQuery()).replaceRelation(es.fromRelation, (es.outRelation));
			}
			execSteps.addAll(eatenEdgesSteps);
			PhysicalQueryVertex singleVertex = (PhysicalQueryVertex) sqv.getVertices().iterator().next();
			if(!sqv.isAggregate()) { // if not aggregate
				PhysicalQueryVertex retVert = null;
				if(!(singleVertex instanceof NDQueryVertex)) { // if distributed
					retVert = PhysicalQueryVertex.newInstance(tableManager.generateTmpTblName(sqv.getAlias()));
					execSteps.add(new StepRunSubquery(sqv.getQuery().toIntermediateString(), false, retVert.getName(), StepPlace.ON_WORKERS));
					if(sqv.getAlias().equals("whole_query")){ // if top level
						NamedRelation gathered = new NamedRelation(tableManager.generateTmpTblName(retVert.getName()));
						execSteps.add(new StepGather(retVert.getName(), gathered.getName()));
						execSteps.add(new StepRunSubquery(sqv.getQuery().toFinalString(gathered), false, tableManager.generateTmpTblName(retVert.getName()), StepPlace.ON_MASTER));
					}
				} else {
					retVert = NDQueryVertex.newInstance(tableManager.generateTmpTblName(sqv.getAlias()));
					execSteps.add(new StepRunSubquery(sqv.getQuery().toUnaliasedString(), false, retVert.getName(), StepPlace.ON_MASTER));
				}
				return retVert;
			}
			NDQueryVertex newVertex = NDQueryVertex.newInstance(tableManager.generateTmpTblName(singleVertex.getName()));
			if(!(singleVertex instanceof NDQueryVertex)) { // if distributed
				String intermediateTableName = tableManager.generateTmpTblName(singleVertex.getName());
				execSteps.add(new StepRunSubquery(sqv.getQuery().toIntermediateString(), true, intermediateTableName, StepPlace.ON_WORKERS));
				NDQueryVertex gathered = NDQueryVertex.newInstance(tableManager.generateTmpTblName(singleVertex.getName()));
				execSteps.add(new StepGather(intermediateTableName, gathered.getName()));
				execSteps.add(new StepRunSubquery(sqv.getQuery().toFinalString(gathered.getRelation()), true, newVertex.getName(), StepPlace.ON_MASTER));
				return newVertex;
			}
			execSteps.add(new StepRunSubquery(sqv.getQuery().toUnaliasedString(), true, newVertex.getName(), StepPlace.ON_MASTER));
			return newVertex;
		}
		
		// We reach here if we have disconnected components.. Ship everything to Master
		for(QueryVertex qv : nodeChildren){
			if(!(qv instanceof NDQueryVertex)) {
				NDQueryVertex gathered = NDQueryVertex.newInstance(tableManager.generateTmpTblName(  ((PhysicalQueryVertex) qv).getName()  ));
				execSteps.add(new StepGather(((PhysicalQueryVertex) qv).getName(), gathered.getName()));
				((QueryRelation) sqv.getQuery()).replaceRelation(((PhysicalQueryVertex) qv).getRelation(), gathered.getRelation());
			}
		}
		if(!sqv.isAggregate()) { // if not aggregate
			PhysicalQueryVertex retVert = null;
			retVert = NDQueryVertex.newInstance(tableManager.generateTmpTblName(sqv.getAlias()));
			execSteps.add(new StepRunSubquery(sqv.getQuery().toUnaliasedString(), false, retVert.getName(), StepPlace.ON_MASTER));
			return retVert;
		}
		PhysicalQueryVertex newVertex = null;
		newVertex = NDQueryVertex.newInstance(tableManager.generateTmpTblName(sqv.getAlias()));
		execSteps.add(new StepRunSubquery(sqv.getQuery().toUnaliasedString(), true, newVertex.getName(), StepPlace.ON_MASTER));
		return newVertex;
	}
	
	/**
	 * Internal helper function - picks one connected D vertex
	 * out of the vertices in the given set
	 * Ultimately it should pick the vertex corresponding 
	 * to the biggest table in the database, so that we minimize 
	 * the number of tuples to be shipped
	 * 
	 * @param vertices the set of vertices to pick from
	 * @return the picked vertex
	 */
	private PhysicalQueryVertex pickConnectedPhysical(Set<QueryVertex> vertices) {
		// TODO pick smartly (e.g. choose largest table, or table at top-most level)
		for(QueryVertex qv : vertices)
			if(!(qv instanceof NDQueryVertex))
				if(edges.get(qv) != null)
					return (PhysicalQueryVertex) qv;
		return null;
	}
	
	/**
	 * Internal helper function - picks one connected ND vertex
	 * out of the vertices in the given set
	 * Ultimately it should pick the vertex corresponding 
	 * to the biggest table in the database, so that we minimize 
	 * the number of tuples to be shipped
	 * 
	 * @param vertices the set of vertices to pick from
	 * @return the picked vertex
	 */
	private PhysicalQueryVertex pickConnectedND(Set<QueryVertex> vertices) {
		// TODO pick smartly (e.g. choose largest table, or table at top-most level)
		for(QueryVertex qv : vertices)
			if(edges.get(qv) != null)
				return (PhysicalQueryVertex) qv;
		return null;
	}
	
	/**
	 * Internal helper function - eats the edge connecting a D node to other nodes
	 * It fuses the two nodes and the resulting operation is a SuperDuper
	 * When an edge is eaten the two vertices are merged meaning that the 
	 * resulting vertex inherits all their links/edges
	 * This function keeps on eating adjacent edges until it cannot do it anymore
	 * In other words it transforms a connected component in the graph into a single 
	 * physical vertex called SuperNode
	 * 
	 * @param vertices the set of vertices from which 
	 * we want to fuse a connected component
	 * @param pqv the physical query vertex picked to start with (meaning 
	 * we start by eating edges adjacent to it and stop when 
	 * we don't have edible edges anymore)
	 * @param execSteps the output of the function meaning the list of 
	 * SuperDuper steps that need to be performed; the number of elements 
	 * in the list must be equal to the number of eaten edges
	 */
	private void eatAllEdgesPhysical(Set<QueryVertex> vertices, PhysicalQueryVertex pqv, List<StepSuperDuper> execSteps) {
		Map<QueryEdge, PhysicalQueryVertex> history = new HashMap<QueryEdge, PhysicalQueryVertex>();
		for(QueryEdge e : edges.get(pqv)) {
			history.put(e, pqv);
		}
		while(edges.get(pqv) != null) {
			QueryEdge edge = edges.get(pqv).get(0);
			PhysicalQueryVertex sp = (PhysicalQueryVertex) edge.getStartPoint();
			PhysicalQueryVertex ep = (PhysicalQueryVertex) edge.getEndPoint();
			PhysicalQueryVertex tempEPTbl = PhysicalQueryVertex.newInstance(tableManager.generateTmpTblName(ep.getName()));
			if(ep instanceof NDQueryVertex) {
				execSteps.add(new StepSuperDuper(ep.getRelation(), history.get(edge).getRelation(), 
						edge.getJoinCondition().getEndPointField(), edge.getJoinCondition().getStartPointField(), 
						true, new NamedRelation(tempEPTbl.getName())));
			} else {
				execSteps.add(new StepSuperDuper(ep.getRelation(), history.get(edge).getRelation(), 
						edge.getJoinCondition().getEndPointField(), edge.getJoinCondition().getStartPointField(), 
						false, new NamedRelation(tempEPTbl.getName())));
			}
			graph.removeEdges(sp, ep);
			for(QueryEdge e : graph.inheritVertex(sp, ep)) {
				history.put(e, tempEPTbl);
			}
			vertices.remove(ep);
		}
	}
	
	/**
	 * Internal helper function - Eats edges connecting ND nodes to other nodes
	 * Since all connected D nodes were exterminated by the previous function
	 * Then this function only handles ND to ND connected nodes and fuses them
	 * This means that there are no resulting operations that need to be performed
	 * 
	 * @param vertices the set of vertices from which we want to fuse vertices
	 * @param pqv the physical query vertex from which to start the eating process
	 * @param execSteps the output which is the list of execution steps that need 
	 * to be performed by eating those edges; as we mentioned in the function 
	 * description this function does not add any execution step because it fuses
	 * ND vertices with ND vertices
	 */
	private void eatAllEdgesND(Set<QueryVertex> vertices, PhysicalQueryVertex pqv, List<StepSuperDuper> execSteps) {
		while(edges.get(pqv) != null) {
			QueryEdge edge = edges.get(pqv).get(0);
			PhysicalQueryVertex sp = (PhysicalQueryVertex) edge.getStartPoint();
			PhysicalQueryVertex ep = (PhysicalQueryVertex) edge.getEndPoint();
			graph.removeEdges(sp, ep);
			graph.inheritVertex(sp, ep);
			vertices.remove(ep);
		}
	}
	
	/**
	 * Helper class - Query Not Supported Exception to be thrown if 
	 * we detect that the submitted query belongs to a family that 
	 * we do not support yet 
	 *
	 * @author Amer C (amer.chamseddine@epfl.ch)
	 *
	 */
	public static class QueryNotSupportedException extends Exception {
		public QueryNotSupportedException(String string) {
			super(string);
		}
		private static final long serialVersionUID = 9133124936966073467L;
	}
	
	/**
	 * Helper class - NDQueryVertex non-distributed physical query vertex
	 * Exactly like physical query vertex
	 * Used in order to differentiate between D and ND
	 *  
	 * @author Amer C (amer.chamseddine@epfl.ch)
	 *
	 */
	static class NDQueryVertex extends PhysicalQueryVertex {
		public static NDQueryVertex newInstance(String name) {
			return new NDQueryVertex(new NamedRelation(name));
		}
		public NDQueryVertex(NamedRelation relation) {
			super(relation);
		}
	}
	
}
