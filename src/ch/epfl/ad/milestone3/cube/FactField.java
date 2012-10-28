package ch.epfl.ad.milestone3.cube;

/**
 * Field storing factual data of the data cube, i.e. the data that will be
 * aggregated on. This class is for integer data.
 * <p>
 * This information is needed to assign proper column names and sizes to
 * relations holding materialized views.
 * 
 * @author Artyom Stetsenko
 * @author tranbaoduy
 * 
 */
public class FactField extends NamedEntity {

	private final int maxValueSize;

	public FactField(String name, String longName, int maxValueSize) {
		super(name, longName);
		this.maxValueSize = maxValueSize;
	}

	public int getMaxValueSize() {
		return this.maxValueSize;
	}

}
