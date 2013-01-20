package ch.epfl.data.zad.milestone3.cube;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

/**
 * ViewPrinter
 *
 * Nicely formats cube views
 *
 * @author Amer C <amer.chamseddine@epfl.ch>
 */

public class ViewPrinter {

	public static final int PRINT_PAGE_SIZE = 10;
	
	public static void printView(View view) throws SQLException, InterruptedException {
		ViewAggStatePrinter.printAggStateData(System.out, view);
		System.out.println("");
		System.out.println("SLICES:");
		if(printSlices(view).size() == 0)
			System.out.println("  (no slices)");
		System.out.println("");
		ViewDataPrinter.printViewData(System.in, System.out, view);
	}
	
	public static List<String> printSlices(View view) throws SQLException, InterruptedException {
		List<String> levNames = new LinkedList<String>();
		int i = 0;
		if(view.getSliceCriteria().size() != 0) {
			for(String levelName : view.getSliceCriteria().keySet()) {
				for(Dimension dim : view.getParentCube().getDimensions().values()) {
					int dimLevel = dim.getLevel(levelName);
					if(dimLevel != Dimension.INVALID_LEVEL) {
						levNames.add(levelName);
						System.out.println("  " + ++i + ". " + dim.getLongName() + "." + dim.getLevel(dimLevel).getLongName() + ": " + view.getSliceCriteria().get(levelName));
						break;
					}
				}
			}
		}
		return levNames;
	}
	
	private static class ViewAggStatePrinter {
		
		public static void printAggStateData(OutputStream os, View view) throws SQLException, InterruptedException {
			Cube cube = view.getParentCube();
			PrintStream printStream = new PrintStream(os);
			
			Map<String, Integer> colWidth = new HashMap<String, Integer>();
			int maxNumLevels = 0;
			for(Dimension dim : cube.getDimensions().values()) {
				if(maxNumLevels < dim.getNumLevels())
					maxNumLevels = dim.getNumLevels();
				int maxLen = dim.getLongName().length() - 4;
				for(Level lev : dim.getHierarchy())
					if(maxLen < lev.getLongName().length())
						maxLen = lev.getLongName().length();
				colWidth.put(dim.getName(), maxLen);
			}
			Map<String, Integer> aggState = view.getAggState();
			
			printStream.print(tblHeader(view, colWidth));
			for(int i = 0; i < maxNumLevels; i++) {
				
				printStream.print("|");
				for(Dimension dim : cube.getDimensions().values()) {
					String levName = (i < dim.getNumLevels() ? dim.getLevel(i).getLongName() : "");
					levName = (i == 0 ? Level.ALL_STRING : levName);
					String prefix = (i == aggState.get(dim.getName()) ? " > " : "   ");
					String suffix = (i == aggState.get(dim.getName()) ? " < " : "   ");
					printStream.print(prefix + PrinterUtils.padRight(levName, colWidth.get(dim.getName())) + suffix);
					printStream.print("|");
				}
				printStream.print("\n");
				
			}
			printStream.print(hrLine(view, colWidth));
		}
		
		private static String tblHeader(View view, Map<String, Integer> colWidth) throws SQLException {
			Cube cube = view.getParentCube();
			StringBuilder hLine = new StringBuilder();
			hLine.append(hrLine(view, colWidth));
			
			hLine.append("|");
			for(Dimension dim : cube.getDimensions().values()) {
				hLine.append(" " + PrinterUtils.padRight(dim.getLongName(), colWidth.get(dim.getName()) + 4) + " ");
				hLine.append("|");
			}
			hLine.append("\n");
			
			hLine.append(hrLine(view, colWidth));
			return hLine.toString();
		}
		
		private static String hrLine(View view, Map<String, Integer> colWidth) throws SQLException {
			Cube cube = view.getParentCube();
			StringBuilder hLine = new StringBuilder();
			hLine.append("+");
			for(Dimension dim : cube.getDimensions().values()) {
				hLine.append("-" + PrinterUtils.padRight("", colWidth.get(dim.getName()) + 4, '-') + "-");
				hLine.append("+");
			}
			hLine.append("\n");
			return hLine.toString();
		}
		
	}
	
	private static class ViewDataPrinter {
		
		public static void printViewData(InputStream is, OutputStream os, View view) throws SQLException, InterruptedException {
			Cube cube = view.getParentCube();
			PrintStream printStream = new PrintStream(os);
			Scanner scanner = new Scanner(is);
			
			Map<String, Integer> colMap = new HashMap<String, Integer>();
			ResultSet result = view.getData();
			ResultSetMetaData meta = result.getMetaData();
			for(int i = 1; i <= meta.getColumnCount(); i++) {
				colMap.put(meta.getColumnLabel(i), i);
			}
			Map<String, Integer> colWidth = new HashMap<String, Integer>();
			List<Map<String, String>> pageData = new LinkedList<Map<String,String>>();
			
			while(true) {
				boolean more = fetchDataPage(view, result, colMap, pageData);
				optimizeWidth(view, pageData, colWidth);
				printStream.print(tblHeader(view, colWidth));
				for(Map<String, String> i : pageData) {
					printStream.print("||");
					for(Dimension dim : cube.getDimensions().values()) {
						for(Level lev : dim.getHierarchy()) {
							printStream.print(" " + PrinterUtils.padRight(i.get(lev.getName()), colWidth.get(lev.getName())) + " ");
							printStream.print("|");
						}
						printStream.print("|");
					}
					for(FactField fact : cube.getFactFields()) {
						printStream.print(" " + PrinterUtils.padLeft(i.get(fact.getName()), colWidth.get(fact.getName())) + " ");
						printStream.print("|");
					}
					printStream.print("|\n");
				}
				printStream.print(hrLine(view, colWidth));
				if(more) {
					printStream.println("Press 'enter' to display next " + PRINT_PAGE_SIZE + " results, or type 'c' then 'enter' to break");
					if(!scanner.nextLine().toLowerCase().startsWith("c"))
						continue;
				}
				break;
			}
		}
		
		private static boolean fetchDataPage(View view, ResultSet result, Map<String, Integer> colMap, List<Map<String, String>> pageData) throws SQLException {
			Cube cube = view.getParentCube();
			pageData.clear();
			for(int i = 0; i < PRINT_PAGE_SIZE; i++) {
				if(!result.next())
					return false;
				Map<String, String> record = new HashMap<String, String>();
				for(Dimension dim : cube.getDimensions().values()) {
					for(Level lev : dim.getHierarchy()) {
						record.put(lev.getName(), result.getString(colMap.get(lev.getName())));
					}
				}
				for(FactField fact : cube.getFactFields()) {
					record.put(fact.getName(), result.getString(colMap.get(fact.getName())));
				}
				pageData.add(record);
			}
			return !result.isLast();
		}
		
		private static void optimizeWidth(View view, List<Map<String, String>> pageData, Map<String, Integer> colWidth) {
			Cube cube = view.getParentCube();
			colWidth.clear();
			for(Dimension dim : cube.getDimensions().values()) {
				for(Level lev : dim.getHierarchy()) {
					colWidth.put(lev.getName(), lev.getLongName().length());
				}
			}
			for(FactField fact : cube.getFactFields()) {
				colWidth.put(fact.getName(), fact.getLongName().length());
			}
			for(Map<String, String> i : pageData) {
				for(Dimension dim : cube.getDimensions().values()) {
					for(Level lev : dim.getHierarchy()) {
						if(i.get(lev.getName()).length() > colWidth.get(lev.getName())) {
							colWidth.remove(lev.getName());
							colWidth.put(lev.getName(), i.get(lev.getName()).length());
						}
					}
				}
				for(FactField fact : cube.getFactFields()) {
					if(i.get(fact.getName()).length() > colWidth.get(fact.getName())) {
						colWidth.remove(fact.getName());
						colWidth.put(fact.getName(), i.get(fact.getName()).length());
					}
				}
			}
		}
		
		private static String tblHeader(View view, Map<String, Integer> colWidth) throws SQLException {
			Cube cube = view.getParentCube();
			StringBuilder hLine = new StringBuilder();
			hLine.append(hrLine(view, colWidth));

			hLine.append("||");
			for(Dimension dim : cube.getDimensions().values()) {
				int parentColWidth = -3;
				for(Level lev : dim.getHierarchy())
					parentColWidth += colWidth.get(lev.getName()) + 3;
				hLine.append(" " + PrinterUtils.padRight(dim.getLongName(), parentColWidth) + " ");
				hLine.append("||");
			}
			int parentColWidth = -3;
			for(FactField fact : cube.getFactFields())
				parentColWidth += colWidth.get(fact.getName()) + 3;
			hLine.append(" " + PrinterUtils.padRight("Facts", parentColWidth) + " ");
			hLine.append("||\n");

			hLine.append("||");
			for(Dimension dim : cube.getDimensions().values()) {
				for(Level lev : dim.getHierarchy()) {
					hLine.append(" " + PrinterUtils.padRight(lev.getLongName(), colWidth.get(lev.getName())) + " ");
					hLine.append("|");
				}
				hLine.append("|");
			}
			for(FactField fact : cube.getFactFields()) {
				hLine.append(" " + PrinterUtils.padRight(fact.getLongName(), colWidth.get(fact.getName())) + " ");
				hLine.append("|");
			}
			hLine.append("|\n");

			hLine.append(hrLine(view, colWidth));
			return hLine.toString();
		}
		
		private static String hrLine(View view, Map<String, Integer> colWidth) throws SQLException {
			Cube cube = view.getParentCube();
			StringBuilder hLine = new StringBuilder();
			hLine.append("++");
			for(Dimension dim : cube.getDimensions().values()) {
				for(Level lev : dim.getHierarchy()) {
					hLine.append("-" + PrinterUtils.padRight("", colWidth.get(lev.getName()), '-') + "-");
					hLine.append("+");
				}
				hLine.append("+");
			}
			for(FactField fact : cube.getFactFields()) {
				hLine.append("-" + PrinterUtils.padRight("", colWidth.get(fact.getName()), '-') + "-");
				hLine.append("+");
			}
			hLine.append("+\n");
			return hLine.toString();
		}
		
	}
	
	public static class PrinterUtils {
		
		public static String padRight(String s, int n, char c) {
			return padRight(s, n).replace(' ', c);  
		}

		public static String padLeft(String s, int n, char c) {
			return padLeft(s, n).replace(' ', c);  
		}
		
		public static String padRight(String s, int n) {
			int x = s.length() - n;
			return String.format("%1$-" + n + "s", (x > 0 ? s.substring(0, n) : s));  
		}

		public static String padLeft(String s, int n) {
			int x = s.length() - n;
			return String.format("%1$#" + n + "s", (x > 0 ? s.substring(x) : s));
		}
		
	}
	
}
