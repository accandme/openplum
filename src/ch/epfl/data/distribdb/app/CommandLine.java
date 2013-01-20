package ch.epfl.data.distribdb.app;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import ch.epfl.data.distribdb.lowlevel.DatabaseManager;
import ch.epfl.data.distribdb.parsing.Parser;
import ch.epfl.data.distribdb.parsing.QueryRelation;
import ch.epfl.data.distribdb.queryexec.ExecStep;
import ch.epfl.data.distribdb.queryexec.GraphProcessor;
import ch.epfl.data.distribdb.queryexec.StepExecutor;
import ch.epfl.data.distribdb.queryexec.GraphProcessor.QueryNotSupportedException;
import ch.epfl.data.distribdb.querytackling.QueryGraph;

public class CommandLine extends AbstractQuery {

	@Override
	public void run(String[] args) throws SQLException, InterruptedException {
		
		if (args.length < 1) {
			System.out.println("Arguments: config-file");
			System.exit(1);
		}
		
        DatabaseManager dbManager = createDatabaseManager(args[0]);
        dbManager.setResultShipmentBatchSize(5000);
		
		System.out.println(
				"Welcome to Distributed SQL Query Engine!\n" +
				"Please enter your query, or type 'exit' when you are finished.\n"
				);
		
		Scanner input = new Scanner(System.in);
		
		while (true) {
			System.out.print(">>> ");
			
			String query = input.nextLine();
			if (query.equalsIgnoreCase("exit")) break;
			else if (query.trim().isEmpty()) continue;
			
			try {
				QueryRelation queryTree = Parser.parse(query);
				if(DEBUG) System.out.println("QUERY: " + queryTree);
				QueryGraph queryGraph = new QueryGraph(queryTree);
				if(DEBUG) System.out.println("QUERY GRAPH:\n" + queryGraph);
				
				try {
					GraphProcessor queryGraphProcessor = new GraphProcessor(queryGraph);
					queryGraphProcessor.processGraph();
					List<ExecStep> execSteps = queryGraphProcessor.getSteps();
					if(DEBUG) System.out.println("QUERY PLAN:");
					if(DEBUG) System.out.println(Arrays.toString(execSteps.toArray()));
					StepExecutor queryStepExecutor = new StepExecutor(dbManager, allNodes);
					queryStepExecutor.setSteps(execSteps);
					queryStepExecutor.executeSteps();
					queryStepExecutor.printResult();
					queryStepExecutor.cleanTempTables();
				} catch (QueryNotSupportedException e) {
					System.out.println("Oh! Ow, this query is not supported :/");
				}
				
				
			} catch (Exception e) {
				System.out.println(e.getMessage() + "\n");
			}
		}
		input.close();
		dbManager.shutDown();
	}
	
	public static void main(String[] args) throws SQLException, InterruptedException {
		new CommandLine().run(args);
	}
}
