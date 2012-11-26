package ch.epfl.ad.db.querytackling;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class ComponentRepresentation {

	private List<Set<PhysicalQueryVertex>> connectedComponents = new LinkedList<Set<PhysicalQueryVertex>>();
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for(Set<PhysicalQueryVertex> s : connectedComponents)
			sb.append(Arrays.toString(s.toArray()) + "\n");
		return sb.toString();
	}
	
	public List<Set<PhysicalQueryVertex>> getComponents() {
		return connectedComponents;
	}
	
	public void addVertex(PhysicalQueryVertex pqv) {
		Set<PhysicalQueryVertex> qc = findComponent(pqv);
		if(qc == null) {
			Set<PhysicalQueryVertex> sp = new HashSet<PhysicalQueryVertex>();
			sp.add(pqv);
			connectedComponents.add(sp);
		}
	}
	
	public void addVertices(PhysicalQueryVertex v1, PhysicalQueryVertex v2) {
		Set<PhysicalQueryVertex> qc1 = findComponent(v1);
		if(qc1 != null) {
			qc1.add(v2);
			return; 
		}
		Set<PhysicalQueryVertex> qc2 = findComponent(v2);
		if(qc2 != null) {
			qc2.add(v1);
			return; 
		}
		Set<PhysicalQueryVertex> sp = new HashSet<PhysicalQueryVertex>();
		sp.add(v1);
		sp.add(v2);
		connectedComponents.add(sp);
	}
	
	private Set<PhysicalQueryVertex> findComponent(PhysicalQueryVertex pqv) {
		for(Set<PhysicalQueryVertex> s : connectedComponents) {
			if(s.contains(pqv))
				return s;
		}
		return null;
	}
	
}
