package ch.epfl.ad.app;

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
 * TablePrinter
 *
 * Nicely formats final result
 *
 * @author Amer C <amer.chamseddine@epfl.ch>
 */

public class TablePrinter {

	public static final int PRINT_PAGE_SIZE = 10;

	public static void printTableData(InputStream is, OutputStream os, ResultSet result) throws SQLException, InterruptedException {

		PrintStream printStream = new PrintStream(os);
		Scanner scanner = new Scanner(is);
		
		ResultSetMetaData meta = result.getMetaData();

		Map<Integer, Integer> colWidth = new HashMap<Integer, Integer>();
		List<Map<Integer, String>> pageData = new LinkedList<Map<Integer,String>>();
		
		while(true) {
			boolean more = fetchDataPage(result, meta, pageData);
			optimizeWidth(pageData, meta, colWidth);
			printStream.print(tblHeader(meta, colWidth));
			for(Map<Integer, String> i : pageData) {
				printStream.print("|");
				for(int j = 1; j <= meta.getColumnCount(); j++) {
					printStream.print(" " + PrinterUtils.padLeft(i.get(j), colWidth.get(j)) + " ");
					printStream.print("|");
				}
				printStream.print("\n");
			}
			printStream.print(hrLine(meta, colWidth));
			if(more) {
				printStream.println("Press 'enter' to display next " + PRINT_PAGE_SIZE + " results, or type 'c' then 'enter' to break");
				if(!scanner.nextLine().toLowerCase().startsWith("c"))
					continue;
			}
			break;
		}
	}
	
	private static boolean fetchDataPage(ResultSet result, ResultSetMetaData meta, List<Map<Integer, String>> pageData) throws SQLException {

		pageData.clear();
		for(int i = 0; i < PRINT_PAGE_SIZE; i++) {
			if(!result.next())
				return false;
			Map<Integer, String> record = new HashMap<Integer, String>();
			for(int j = 1; j <= meta.getColumnCount(); j++) {
				record.put(j, result.getString(j));
			}
			pageData.add(record);
		}
		return !result.isLast();
	}
	
	private static void optimizeWidth(List<Map<Integer, String>> pageData, ResultSetMetaData meta, Map<Integer, Integer> colWidth) throws SQLException {

		colWidth.clear();
		for(int j = 1; j <= meta.getColumnCount(); j++) {
			colWidth.put(j, meta.getColumnLabel(j).length());
		}
		for(Map<Integer, String> i : pageData) {
			for(int j = 1; j <= meta.getColumnCount(); j++) {
				if(i.get(j).length() > colWidth.get(j)) {
					colWidth.remove(j);
					colWidth.put(j, i.get(j).length());
				}
			}
		}
	}
	
	private static String tblHeader(ResultSetMetaData meta, Map<Integer, Integer> colWidth) throws SQLException {
		StringBuilder hLine = new StringBuilder();
		hLine.append(hrLine(meta, colWidth));

		hLine.append("|");
		for(int j = 1; j <= meta.getColumnCount(); j++) {
			hLine.append(" " + PrinterUtils.padRight(meta.getColumnLabel(j), colWidth.get(j)) + " ");
			hLine.append("|");
		}
		hLine.append("\n");

		hLine.append(hrLine(meta, colWidth));
		return hLine.toString();
	}
	
	private static String hrLine(ResultSetMetaData meta, Map<Integer, Integer> colWidth) throws SQLException {
		
		StringBuilder hLine = new StringBuilder();
		hLine.append("+");

		for(int j = 1; j <= meta.getColumnCount(); j++) {
			hLine.append("-" + PrinterUtils.padRight("", colWidth.get(j), '-') + "-");
			hLine.append("+");
		}
		hLine.append("\n");
		return hLine.toString();
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
