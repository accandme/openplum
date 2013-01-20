package ch.epfl.data.zad.milestone3.cube;

/**
 * Factual data of the data cube, i.e. the data that will be aggregated on.
 * This class is for decimal data. It holds two measures of the stored
 * decimal values: precision and scale, which correspond to the measures
 * required by MySQL to specify data of type DOUBLE.
 * <p>
 * This information is needed to assign proper column names and
 * sizes to relations holding materialized views.
 * 
 * @author Artyom Stetsenko
 *
 */
public class DoubleFactField extends FactField {
	
	/**
	 * The precision (maximum number of digits) of the resulting DOUBLE.
	 */
	private final int precision;
	
	/**
	 * The scale (number of digits to the right of the decimal point)
	 * of the resulting DOUBLE.
	 */
	private final int scale;

	/**
	 * Constructor.
	 * 
	 * @param name      short name of the fact item.
	 * @param longName  long, descriptive name of the fact item.
	 * @param precision precision of the fact item's value.
	 * @param scale     scale of the fact item's value.
	 */
	public DoubleFactField(String name, String longName, int precision, int scale) {
		super(name, longName, precision + 1);
		this.precision = precision;
		this.scale = scale;
	}
	
	/**
	 * Gets the precision of the fact item's value.
	 * 
	 * @return the precision of this item's value
	 */
	public int getPrecision() {
		return this.precision;
	}
	
	/**
	 * Gets the scale of the fact item's value.
	 * 
	 * @return the scale of this item's value
	 */
	public int getScale() {
		return this.scale;
	}
}
