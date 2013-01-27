package ch.epfl.data.distribdb.app;

import java.io.FileReader;
import java.sql.SQLException;
import java.util.List;
import java.util.Scanner;

import ch.epfl.data.distribdb.execution.ExecStep;
import ch.epfl.data.distribdb.execution.GraphProcessor;
import ch.epfl.data.distribdb.execution.StepExecutor;
import ch.epfl.data.distribdb.execution.TableManager;
import ch.epfl.data.distribdb.lowlevel.DatabaseManager;
import ch.epfl.data.distribdb.parsing.Parser;
import ch.epfl.data.distribdb.parsing.QueryRelation;
import ch.epfl.data.distribdb.tackling.QueryGraph;

public class QueryTimer extends AbstractApp {

	DatabaseManager dbManager;
	
	@Override
	public void run(String[] args) throws SQLException, InterruptedException {
		
		if (args.length < 1) {
			System.out.println("Arguments: config-file");
			System.exit(1);
		}
		
		dbManager = createDatabaseManager(args[0]);
		dbManager.setResultShipmentBatchSize(5000);
		TableManager tableManager = new TableManager();
		Parser.parse("select 1 as bla from parser_needs_to_warm_up");
		
		try {
			Scanner input = new Scanner(new FileReader("queries.sql"));
			while (input.hasNext()) {
				String query = input.nextLine();
				if (!query.toLowerCase().startsWith("select")) continue;
				
				long startTime = System.currentTimeMillis();
				
				QueryRelation queryTree = Parser.parse(query);
				QueryGraph queryGraph = new QueryGraph(queryTree);
				GraphProcessor queryGraphProcessor = new GraphProcessor(tableManager, queryGraph);
				List<ExecStep> execSteps = queryGraphProcessor.processGraph();
				StepExecutor queryStepExecutor = new StepExecutor(dbManager, tableManager, allNodes);
				queryStepExecutor.executeSteps(execSteps);
				
				System.out.println((System.currentTimeMillis() - startTime) / 1000.0);
					
			}
			input.close();
		} catch (Exception e1) {
			e1.printStackTrace();
		} finally {
			tableManager.cleanTempTables(dbManager, allNodes);
		}
		
		dbManager.shutDown();
	}
	
	public static void main(String[] args) throws SQLException, InterruptedException {
		new QueryTimer().run(args);
	}
}
