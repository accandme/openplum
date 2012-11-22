package ch.epfl.ad.db.querytackling;

public class PhysicalQueryVertex extends QueryVertex {
	
	private String name;
	
	public PhysicalQueryVertex(String name) {
		if (name == null) {
			throw new IllegalArgumentException("Physical query vertex name cannot be null.");
		}
		this.name = name;
	}
	
	public String getName() {
		return this.name;
	}
	
	@Override
	public String toString() {
		return this.name;
	}
}
