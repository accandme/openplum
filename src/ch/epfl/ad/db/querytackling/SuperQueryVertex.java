package ch.epfl.ad.db.querytackling;

import java.util.Set;

public class SuperQueryVertex extends QueryVertex {
	
	private String alias;
	private Set<QueryVertex> vertices;
	
	public SuperQueryVertex(String alias, Set<QueryVertex> vertices) {
		if (alias == null) {
			throw new IllegalArgumentException("Supervertex alias cannot be null.");
		}
		if (vertices == null || vertices.size() == 0) {
			throw new IllegalArgumentException("Supervertex must contain at least one vertex.");
		}
		this.alias = alias;
		this.vertices = vertices;
	}
	
	public String getAlias() {
		return this.alias;
	}
	
	public Set<QueryVertex> getVertices() {
		return this.vertices;
	}
	
	@Override
	public String toString() {
		StringBuilder string = new StringBuilder(this.alias).append("{");
		String prefix = "";
		for (QueryVertex vertex : this.vertices) {
			string.append(prefix);
			string.append(vertex);
			prefix = ", ";
		}
		string.append("}");
		return string.toString();
	}
}
