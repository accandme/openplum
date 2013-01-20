package ch.epfl.data.distribdb.lowlevel;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * This interface defines a proxy API for all standard JDBC database accesses
 * required by our milestone 2. The implementation can be either sequential or
 * parallel.
 * 
 * @author tranbaoduy
 * 
 */
public interface DatabaseManager {

    /**
     * Connects to a node at the given JDBC URL and assign the given node ID to
     * that connection.
     * 
     * @param nodeId
     *            Node ID
     * @param jdbcUrl
     *            JDBC URL
     * 
     * @throws SQLException
     */
    public void connect(String nodeId, String jdbcUrl, String username,
            String password) throws SQLException;

    /**
     * Disconnects from the node specified by the given ID.
     * 
     * @param nodeId
     *            Node ID
     * @throws SQLException
     */
    public void disconnect(String nodeId) throws SQLException;

    /**
     * Gets all node names.
     * 
     * @return Node names
     */
    public List<String> getNodeNames();

    /**
     * Gets current number of nodes.
     * 
     * @return Number of nodes
     */
    public int getNumNodes();

    /**
     * Executes the given query on the source node and deposits the results into
     * the given table on the destination node.
     * <p>
     * The result table schema should be specified as a string in the following
     * format: table-name OR table-name(field-1, field-2, ...).
     * 
     * @param query
     *            Single query string
     * @param sourceNodeId
     *            Single source node ID
     * @param resultTableSchema
     *            Single schema for result table
     * @param destinationNodeId
     *            Single destination node IDs
     * 
     * @throws SQLException
     */
    public void execute(String query, String sourceNodeId,
            String resultTableSchema, String destinationNodeId)
            throws SQLException;

    /**
     * Executes the given query on multiple source nodes and deposits all
     * results into the given table on one single destination node.
     * <p>
     * The result table schema should be specified as a string in the following
     * format: table-name OR table-name(field-1, field-2, ...).
     * 
     * @param query
     *            Single query string
     * @param sourceNodeId
     *            Single source node ID
     * @param resultTableSchema
     *            Single schema for result table
     * @param destinationNodeId
     *            Single destination node IDs
     * 
     * @throws SQLException
     */
    public void execute(String query, List<String> sourceNodeIds,
            String resultTableSchema, String destinationNodeId)
            throws SQLException, InterruptedException;

    /**
     * Executes the given query on the source node and deposits (replicates) the
     * results into the same table on several destination nodes.
     * <p>
     * The result table schema should be specified as a string in the following
     * format: table-name OR table-name(field-1, field-2, ...).
     * 
     * @param query
     *            Single query string
     * @param sourceNodeId
     *            Single source node ID
     * @param resultTableSchema
     *            Single schema for result table
     * @param destinationNodeIds
     *            Multiple destination node IDs
     * 
     * @throws SQLException
     * @throws InterruptedException
     */
    public void execute(String query, String sourceNodeId,
            String resultTableSchema, List<String> destinationNodeIds)
            throws SQLException, InterruptedException;

    /**
     * Executes the given query on multiple source nodes and deposits the
     * results into multiple tables, respectively to the source nodes.
     * <p>
     * The result table schemata should be a string in the following format:
     * table-name OR table-name(field-1, field-2, ...), and specified in the
     * respective order to source node IDs.
     * <p>
     * The number of source nodes and the number of result table schemata should
     * match. Otherwise, extra items are ignored.
     * 
     * @param query
     *            Single query string
     * @param sourceNodeIds
     *            Multiple source node IDs
     * @param resultTableSchemata
     *            Single schema for result table
     * @param destinationNodeId
     *            Single destination node ID
     * 
     * @throws SQLException
     * @throws InterruptedException
     */
    public void execute(String query, List<String> sourceNodeIds,
            List<String> resultTableSchemata, String destinationNodeId)
            throws SQLException, InterruptedException;

    /**
     * Executes the given query on multiple source nodes and deposits the
     * results into multiple tables, respectively to the source nodes. These set
     * of tables are then replicated across all destination nodes.
     * <p>
     * The result table schemata should be a string in the following format:
     * table-name OR table-name(field-1, field-2, ...), and specified in the
     * respective order to source node IDs.
     * <p>
     * The number of source nodes and the number of result table schemata should
     * match. Otherwise, extra items are ignored.
     * 
     * The number of destination nodes are unrelated and can be different.
     * 
     * @param query
     *            Single query string
     * @param sourceNodeId
     *            Multiple source node IDs
     * @param resultTableSchemata
     *            Multiple schemata for result tables
     * @param destinationNodeIds
     *            Multiple destination node IDs
     * 
     * @throws SQLException
     * @throws InterruptedException
     */
    public void execute(String query, List<String> sourceNodeIds,
            List<String> resultTableSchemata, List<String> destinationNodeIds)
            throws SQLException, InterruptedException;

    /**
     * Executes multiple queries on a single source node and deposits the result
     * into tables located on respective destination nodes according to the
     * original queries.
     * <p>
     * The result table schemata should be a string in the following format:
     * table-name OR table-name(field-1, field-2, ...), and specified in the
     * respective order to the queries.
     * <p>
     * The number of queries, the number of result table schemata and the number
     * of destination nodes should match. Otherwise, extra items are ignored.
     * 
     * @param queries
     *            Multiple query strings
     * @param sourceNodeId
     *            Single source node ID
     * @param resultTableSchemata
     *            Multiple schemata for result table
     * @param destinationNodeIds
     *            destination node IDs
     * 
     * @throws SQLException
     * @throws InterruptedException
     */
    public void execute(List<String> queries, String sourceNodeId,
            List<String> resultTableSchemata, List<String> destinationNodeIds)
            throws SQLException, InterruptedException;

    /**
     * Execute a single (update) query on the node.
     * 
     * @param query
     *            Single (update) query
     * @param nodeId
     *            Single node ID
     * 
     * @throws SQLException
     */
    public void execute(String query, String nodeId) throws SQLException;

    /**
     * Execute a single (update) query on multiple nodes.
     * 
     * @param query
     *            Single (update) query
     * @param nodeIds
     *            Multiple node IDs
     * 
     * @throws SQLException
     * @throws InterruptedException
     */
    public void execute(String query, List<String> nodeIds)
            throws SQLException, InterruptedException;

    /**
     * Execute a single 'fetch' (non-update) query on the specified node and
     * return the result set.
     * 
     * @param query
     *            Single query
     * @param nodeId
     *            Single node ID
     * 
     * @return Standard JDBC result set
     * 
     * @throws SQLException
     */
    public ResultSet fetch(String query, String nodeId) throws SQLException;

    /**
     * Execute multiple 'fetch' (non-update) queries on the specified node and
     * return the result sets in respective order.
     * 
     * @param query
     *            Single query
     * @param nodeId
     *            Multiple node IDs
     * 
     * @return Multiple standard JDBC result sets
     * 
     * @throws SQLException
     * @throws InterruptedException
     */
    public List<ResultSet> fetch(String query, List<String> nodeIds)
            throws SQLException, InterruptedException;

    /**
     * Execute a single query on multiple nodes and store the results in
     * temporary table on the same nodes.
     * <p>
     * The result table schemata should be a string in the following format:
     * table-name OR table-name(field-1, field-2, ...).
     * 
     * @param query
     *            Single (update) query
     * @param resultTableSchema
     *            Single schema for result table
     * @param nodeId
     *            Single node ID
     * 
     * @throws SQLException
     */
    public void execute(String query, List<String> nodeIds,
            String resultTableSchema) throws SQLException, InterruptedException;

    /**
     * Execute a single query on the node and store the result in temporary
     * table on the same node.
     * <p>
     * The result table schemata should be a string in the following format:
     * table-name OR table-name(field-1, field-2, ...).
     * 
     * @param query
     *            Single (update) query
     * @param resultTableSchema
     *            Single schema for result table
     * @param nodeId
     *            Single node ID
     * 
     * @throws SQLException
     */
    public void execute(String query, String nodeId, String resultTableSchema)
            throws SQLException, InterruptedException;

    /**
     * Copy the whole source relation (on the source node) to the target
     * relation (on the destination node).
     * <p>
     * The target table schemata should be a string in the following format:
     * table-name OR table-name(field-1, field-2, ...).
     * 
     * @param sourceRelationName
     *            Source relation name
     * @param sourceNodeId
     *            Source node ID
     * @param targetRelationSchema
     *            Target relation schema
     * @param destinationNodeId
     *            Destination node ID
     */
    public void copyTable(String sourceRelationName, String sourceNodeId,
            String targetRelationName, String destinationNodeId)
            throws SQLException;

    /**
     * Shuts down this DatabaseManager.
     */
    public void shutDown();

    /**
     * Sets the batch size for data shipment (number of tuples for each INSERT
     * query typically used to ship results from source node(s) to destination
     * node(s)).
     * 
     * @param batchSize
     *            Batch size (no. of tuples, must be >=0; default setting is
     *            zero, meaning no splitting into batches)
     */
    public void setResultShipmentBatchSize(int batchSize);

    /**
     * Gets the batch size for data shipment (number of tuples for each INSERT
     * query typically used to ship results from source node(s) to destination
     * node(s)).
     * 
     * @return Batch size (no. of tuples)
     */
    public int getResultShipmentBatchSize();
}
