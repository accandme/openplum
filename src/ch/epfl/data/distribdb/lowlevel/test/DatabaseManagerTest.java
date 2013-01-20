package ch.epfl.data.distribdb.lowlevel.test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import ch.epfl.data.distribdb.lowlevel.DatabaseManager;
import ch.epfl.data.distribdb.lowlevel.SequentialDatabaseManager;

public class DatabaseManagerTest {

    public static void main(String[] args) throws ClassNotFoundException,
            SQLException, InterruptedException {

        Class.forName("com.mysql.jdbc.Driver");
        DatabaseManager dbMgr = new SequentialDatabaseManager();
        
        List<String> allNodes = Arrays.asList("node1", "node2", "node3", "node4", "node5", "node6", "node7", "node8");
        String username = "team9";
		String password = "thrufbeyto";
        
        // Some testing goes here...
        
		dbMgr.connect("node3","jdbc:mysql://icdatasrv2-9:3306/dbcourse1", username, password);
        dbMgr.connect("node1", "jdbc:mysql://icdatasrv1-9:3306/dbcourse1", username, password);
        dbMgr.connect("node2", "jdbc:mysql://icdatasrv1-20:3306/dbcourse1", username, password);
        dbMgr.connect("node4", "jdbc:mysql://icdatasrv2-20:3306/dbcourse1", username, password);
        dbMgr.connect("node5", "jdbc:mysql://icdatasrv3-9:3306/dbcourse1", username, password);
        dbMgr.connect("node6", "jdbc:mysql://icdatasrv3-20:3306/dbcourse1", username, password);
        dbMgr.connect("node7", "jdbc:mysql://icdatasrv4-9:3306/dbcourse1", username, password);
        dbMgr.connect("node8", "jdbc:mysql://icdatasrv4-20:3306/dbcourse1", username, password);
		
        //test2 execute(String query, List<String> nodeIds)
        //
		//dbMgr.execute("CREATE TABLE temp_nation (n_name CHAR(25), n_nationkey INTEGER)", allNodes);
                
        //test2 clean
		//
		//dbMgr.execute("DROP TABLE temp_nation", allNodes);
        	
        //test3 execute(String query, List<String> sourceNodeIds, String resultTableSchema, String destinationNodeId)
		//
		//dbMgr.execute("CREATE TABLE some_table(name CHAR(30))", "node7");
		//dbMgr.execute("SELECT n_name FROM nation WHERE n_name='VIETNAM'", allNodes, "some_table", "node7");
        
        //test3 clean
        //
        dbMgr.execute("DROP TABLE some_table", "node7");
        
        //test4 execute(String query, List<String> sourceNodeIds, List<String> resultTableSchemata, String destinationNodeId)
        //
        dbMgr.execute("CREATE TABLE table1(name CHAR(30))","node7");
        dbMgr.execute("CREATE TABLE table2(name CHAR(30))","node7");
        List<String> listTables = Arrays.asList("table1", "table2");
        dbMgr.execute("SELECT n_name FROM nation WHERE n_name='VIETNAM'", allNodes, listTables,"node7");
        //
		//!!!!! problem: table1 consist of one VIETNAM tuple, table2 is empty
        
        //test4 clean
        //
        //dbMgr.execute("DROP TABLE table1", "node7");
        //dbMgr.execute("DROP TABLE table2", "node7");
        
        //test5 execute(String query, List<String> sourceNodeIds, List<String> resultTableSchemata, List<String> destinationNodeIds)
        //
        //dbMgr.execute("CREATE TABLE table1(name CHAR(30))",allNodes);
        //dbMgr.execute("CREATE TABLE table2(name CHAR(30))",allNodes);
        //List<String> listTables = Arrays.asList("table1", "table2");
        //dbMgr.execute("SELECT n_name FROM nation WHERE n_name='VIETNAM'", allNodes, listTables, allNodes);
        //!!!!! problem: table1 consist of one VIETNAM tuple, table2 is empty on all nodes
        
        //test5 clean
        //
        //dbMgr.execute("DROP TABLE table1", allNodes);
        //dbMgr.execute("DROP TABLE table2", allNodes);
        
        //test6 execute(List<String> queries, String sourceNodeId, List<String> resultTableSchemata, List<String> destinationNodeIds)
        //
        //List<String> listQueries = Arrays.asList("SELECT n_name FROM nation WHERE n_name='VIETNAM'",
        //										 "SELECT table2");
        
        
        dbMgr.disconnect("node1");
        dbMgr.disconnect("node2");
        dbMgr.disconnect("node3");
        dbMgr.disconnect("node4");
        dbMgr.disconnect("node5");
        dbMgr.disconnect("node6");
        dbMgr.disconnect("node7");
        dbMgr.disconnect("node8");		
		
    }
}
