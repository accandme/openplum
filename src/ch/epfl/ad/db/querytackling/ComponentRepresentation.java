package ch.epfl.ad.db.querytackling;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

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
	
	public PhysicalQueryVertex getRoot() {
		if(connectedComponents.size() == 0)
			return null;
		//Set<PhysicalQueryVertex> set = null;
		Collections.sort(connectedComponents, new Comparator<Set<PhysicalQueryVertex>>() {
			public int compare(Set<PhysicalQueryVertex> o1, Set<PhysicalQueryVertex> o2) {
				// want biggest first
				return new Integer(o2.size()).compareTo(new Integer(o1.size()));
			}
		});
		Set<PhysicalQueryVertex> set = connectedComponents.get(0);
		if(set.size() == 0)
			return null;
		SortedSet<PhysicalQueryVertex> ee = new TreeSet<PhysicalQueryVertex>(new Comparator<PhysicalQueryVertex>() {
			public int compare(PhysicalQueryVertex o1, PhysicalQueryVertex o2) {
				// want biggest first
				// TODO actually check the number of rows in the table, not the number of characters in the name!
				return new Integer(o2.getName().length()).compareTo(new Integer(o1.getName().length()));
			}
		});
		ee.addAll(set);
		return ee.first();
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
