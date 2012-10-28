package ch.epfl.ad.milestone3.app;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import ch.epfl.ad.milestone3.cube.Dimension;
import ch.epfl.ad.milestone3.cube.DoubleFactField;
import ch.epfl.ad.milestone3.cube.FactField;
import ch.epfl.ad.milestone3.cube.Level;

/**
 * Holds settings for the cube, so that they can be reused across different
 * programs.
 * @author Anton Dimitrov
 *
 */
public class CubeSettings {

	public static Set<Dimension> getDimensions() {
		Set<Dimension> dimensions = new HashSet<Dimension>(Arrays.asList(
				new Dimension("dim_parts", "Part", Arrays.asList(
						new Level("part_mfgr", "Manufacturer", 25),
						new Level("part_brand", "Brand", 10),
						new Level("part_name", "Name", 55)
						)),
				new Dimension("dim_customers", "Customer", Arrays.asList(
						new Level("customer_region", "Region", 25),
						new Level("customer_nation", "Nation", 25),
						new Level("customer_name", "Name", 25)
						)),
				new Dimension("dim_suppliers", "Supplier", Arrays.asList(
						new Level("supplier_region", "Region", 25),
						new Level("supplier_nation", "Nation", 25),
						new Level("supplier_name", "Name", 25)
						)),
				new Dimension("dim_time", "Time", Arrays.asList(
						new Level("time_year", "Year", 4),
						new Level("time_month", "Month", 2),
						new Level("time_day", "Day", 2)
						))
				));

		return dimensions;
	}
	
	public static List<FactField> getFactFields() {
		List<FactField> factFields = Arrays.asList(
				new FactField("quantity", "Quantity", 10),
				new DoubleFactField("extendedprice", "Price", 15, 2)
				);
		return factFields;
	}
}
