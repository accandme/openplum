package ch.epfl.ad.milestone3.app;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import ch.epfl.ad.AbstractQuery;
import ch.epfl.ad.db.DatabaseManager;
import ch.epfl.ad.milestone3.cube.Cube;
import ch.epfl.ad.milestone3.cube.Dimension;
import ch.epfl.ad.milestone3.cube.Level;
import ch.epfl.ad.milestone3.cube.Materializer;
import ch.epfl.ad.milestone3.cube.ViewPrinter;

/**
 * Command Line Interpreter
 * 
 * @author Stanislav Peshterliev
 * @author Amer C
 * @author tranbaoduy
 */
public class CommandLineCube extends AbstractQuery {
	
	static String help = "Welcome to Command Line Cube! \n" + 
	"List of commands: \n" +
	"drillDown - drill down current view on dimension\n" +
    "rollUp - roll up current view on dimension\n" +
	"addSlice - slice current view on certain level\n" +
	"removeSlice - remove a slice\n" +
	"undo - undo the last command\n" +
	"redo - redo the last command\n" +
	"help - shows this help \n" +
	"exit - leave the program";

	@Override
	public void run(String[] args) throws SQLException, InterruptedException {
		if (args.length < 1) {
			System.out.println("Arguments: config-file");
			System.exit(1);
		}
		
        DatabaseManager dbManager = createDatabaseManager(args[0]);
        dbManager.setResultShipmentBatchSize(5000);

        Set<Dimension> dimensions = CubeSettings.getDimensions();
        Cube salesCube = new Cube("view_fact_sales", dimensions,
                CubeSettings.getFactFields(), dbManager);

        salesCube.addMaterializedViews(Materializer.getPreloadedViews(salesCube,
                "materialized_views_catalog", dbManager.getNodeNames().get(0)));		

		Scanner s = null;

		try {
			s = new Scanner(System.in);

			String command = "";
			String argument = ""; 
			List<String> parts = null;
			
			System.out.println(help);
			
			ViewPrinter.printView(salesCube.getCurView());
			
			while (!command.equals("exit")) {
				System.out.print(">>> ");
				
				parts = Arrays.asList(s.nextLine().split(" "));
								
				command = parts.get(0);
				argument = "";
					
				if (parts.size() > 1) {
					argument = parts.subList(1, parts.size()).toString().replaceAll(", ", " ").replaceAll("^\\[|\\]$", "");
				}
				
				if (command.equals("help")) {
					System.out.println(help);
					
				} else if (command.equals("drillDown")) {
					try {
						argument = askForDimension(salesCube, argument);
				        salesCube.drillDown(argument);
				        ViewPrinter.printView(salesCube.getCurView());
					} catch (IllegalArgumentException e) {
						System.out.println("Error: " + e.getMessage());
					} catch (IllegalStateException e) {
						System.out.println("Error: " + e.getMessage());
					}
				} else if (command.equals("rollUp")) {
                    try {
						argument = askForDimension(salesCube, argument);
                        salesCube.rollUp(argument);
                        ViewPrinter.printView(salesCube.getCurView());
                    } catch (IllegalArgumentException e) {
                        System.out.println("Error: " + e.getMessage());
                    } catch (IllegalStateException e) {
                        System.out.println("Error: " + e.getMessage());
                    }
                } else if (command.equals("addSlice")) {
					try {
						argument = askForLevel(salesCube);
						System.out.println("Now type any expression using $ to refer to the chosen level (e.g. $ >=1995 AND $ < 2000)");
				        salesCube.addSlice(argument, s.nextLine());
				        ViewPrinter.printView(salesCube.getCurView());
                    } catch (IllegalArgumentException e) {
                        System.out.println("Error: " + e.getMessage());
					} catch (SQLException e) {
						System.out.println("Error: " + e.getMessage());
					}
                } else if (command.equals("removeSlice")) {
					try {
						argument = askForSlice(salesCube);
				        salesCube.removeSlice(argument);
				        ViewPrinter.printView(salesCube.getCurView());
                    } catch (IllegalArgumentException e) {
                        System.out.println("Error: " + e.getMessage());
					} catch (SQLException e) {
						System.out.println("Error: " + e.getMessage());
					}
				} else if (command.equals("undo")) {
					try {
				        salesCube.undo();
				        ViewPrinter.printView(salesCube.getCurView());
					} catch (IllegalStateException e) {
						System.out.println("Error: " + e.getMessage());
					}
				} else if (command.equals("redo")) {
                    try {
                        salesCube.redo();
                        ViewPrinter.printView(salesCube.getCurView());
                    } catch (IllegalStateException e) {
                        System.out.println("Error: " + e.getMessage());
                    }
				} else if (command.equals("exit") || command.equals("")) {
					;
				} else {
					System.out.println("Error: Unrecognized command");
				}
			}
			
			System.out.println("See you next time!");
			
		} finally {
			if (s != null) {
				s.close();
			}
		}
		
		dbManager.shutDown();
	}
	
	private String askForDimension(Cube cube, String argument) {
		if(argument.length() != 0)
			return argument;
		try {
			Scanner s = new Scanner(System.in);
			List<Dimension> dims = new ArrayList<Dimension>(cube.getDimensions().values());
			System.out.println("");
			for(int i = 0; i < dims.size(); i++) {
				System.out.println("  " + (i + 1) + ". " + dims.get(i).getLongName());
			}
			System.out.println("");
			System.out.println("Please select a dimension by specifying its number");
			argument = dims.get(Integer.parseInt(s.nextLine()) - 1).getName();
			return argument;
		} catch (Exception e) {
			throw new IllegalArgumentException(e.getMessage());
		} 
	}

	private String askForLevel(Cube cube) {
		try {
			Scanner s = new Scanner(System.in);
			Map<String, Integer> aggState = cube.getCurView().getAggState();
			Map<String, String> sliceCriteria = cube.getCurView().getSliceCriteria();
			List<String> levNames = new LinkedList<String>();
			int i = 0;
			System.out.println("");
			for(Dimension dim : cube.getDimensions().values()) {
				for(Level lev : dim.getHierarchy()) {
					if(dim.getLevel(lev.getName()) <= aggState.get(dim.getName())) {
						if(!sliceCriteria.containsKey(lev.getName())) {
							levNames.add(lev.getName());
							System.out.println("  " + ++i + ". " + dim.getLongName() + "." + lev.getLongName());
						}
					}
				}
			}
			if(levNames.size() == 0)
				throw new IllegalArgumentException("You cannot slice further, drillDown or remove some slicing conditions!");
			System.out.println("");
			System.out.println("Choose Dimension/Level to slice on, by specifying its number");
			return levNames.get(Integer.parseInt(s.nextLine()) - 1);
		} catch (Exception e) {
			throw new IllegalArgumentException(e.getMessage());
		} 
	}

	private String askForSlice(Cube cube) {
		try {
			Scanner s = new Scanner(System.in);
			System.out.println("");
			List<String> levNames = ViewPrinter.printSlices(cube.getCurView());
			System.out.println("");
			if(levNames.size() == 0)
				throw new IllegalArgumentException("There are no slices!");
			System.out.println("Choose slice to remove by specifying its number");
			return levNames.get(Integer.parseInt(s.nextLine()) - 1);
		} catch (Exception e) {
			throw new IllegalArgumentException(e.getMessage());
		} 
	}

	public static void main(String[] args) throws SQLException,
			InterruptedException {
		new CommandLineCube().run(args);
	}
}
