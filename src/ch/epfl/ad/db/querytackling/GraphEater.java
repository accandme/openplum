package ch.epfl.ad.db.querytackling;

import java.util.List;
import java.util.Map.Entry;

public class GraphEater {
	
	ComponentRepresentation connectedComponents = new ComponentRepresentation();
	PhysicalQueryVertex lastEater = null;
	boolean rootOnly = false;
	
	public GraphEater() {
	}
	
	public GraphEater(PhysicalQueryVertex rootVertex) {
		rootOnly = true;
		this.lastEater = rootVertex;
	}
	
	public void eat(QueryGraph graph) {
		boolean ateEdge;
		boolean fusedBubble;
		do {
			ateEdge = eatSomeEdge(graph);
			fusedBubble = fuseSomeBubble(graph);
		} while(ateEdge || fusedBubble);
		for(PhysicalQueryVertex qv : graph.getPhysicalVerticesRecursively()) {
			connectedComponents.addVertex(qv);
		}
		System.out.println("FINAL OUTPUT #####");
		System.out.println(connectedComponents);
	}
	
	private boolean eatSomeEdge(QueryGraph graph) {
		if(lastEater != null && graph.getVertexEdges(lastEater) != null) {
			System.out.println("CONTINUING " + lastEater);
			if(eatSomeEdgeFromSet(graph, graph.getVertexEdges(lastEater)))
				return true;
		}
		if(rootOnly)
			return false;
		for(Entry<QueryVertex, List<QueryEdge>> bla : graph.getEdges().entrySet()) {
			if(eatSomeEdgeFromSet(graph, bla.getValue()))
				return true;
		}
		return false;
	}
	
	private boolean eatSomeEdgeFromSet(QueryGraph graph, List<QueryEdge> edges) {
		for(QueryEdge edge : edges) {
			if(edge.getStartPoint() instanceof PhysicalQueryVertex && edge.getEndPoint() instanceof PhysicalQueryVertex) {
				eatEdge(graph, edge);
				return true;
			}
		}
		return false;
	}
	
	private boolean fuseSomeBubble(QueryGraph graph) { // TODO loop on vertices as well
		for(QueryVertex v : graph.getEdges().keySet()) {
			if(v instanceof SuperQueryVertex) {
				SuperQueryVertex sqv = (SuperQueryVertex) v;
				if(sqv.getVertices().size() == 1) {
					fuseBubble(graph, sqv);
					return true;
				}
			}
		}
		return false;
	}

	private void eatEdge(QueryGraph graph, QueryEdge edge) {
		PhysicalQueryVertex sp = (PhysicalQueryVertex) edge.getStartPoint();
		PhysicalQueryVertex ep = (PhysicalQueryVertex) edge.getEndPoint();
		// TODO if ep is larger than than sp then swap them
		// TODO IMPORTANT if edge crosses a bubble boundary then we might not be able to remove it
		// TODO IMPORTANT if one end is already super-duper-ed then we are forced to ship to it
		System.out.println("COMMAND ##### SuperDuper: ship " + ep + " to " + sp);
		lastEater = sp;
		connectedComponents.addVertices(sp, ep);
		graph.removeEdge(sp, ep);
		graph.inheritVertex(sp, ep);
		if(!graph.removeVertex(ep))
			System.out.println("WARNING could not delete vertex :-(");
		System.out.println("NEW STATE #####");
		System.out.println(graph);
	}
	
	private void fuseBubble(QueryGraph graph, SuperQueryVertex sqv) {
		graph.inheritVertex(sqv.getVertices().iterator().next(), sqv);
		if(!graph.removeVertex(sqv))
			System.out.println("WARNING could not delete vertex :-(");
		System.out.println("NEW STATE #####");
		System.out.println(graph);
	}

}
