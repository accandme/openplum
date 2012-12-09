package ch.epfl.ad.db.parsing;

public enum Aggregate {
	
	SUM ("SUM"),
	AVG ("AVG"),
	COUNT ("COUNT"),
	MIN ("MIN"),
	MAX ("MAX");
	
	private final String aggregate;
	
	private Aggregate(String aggregate) {
		this.aggregate = aggregate;
	}
	
	@Override
	public String toString() {
		return this.aggregate;
	}
}
