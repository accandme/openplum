package ch.epfl.ad.db;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * An implementation of DatabaseManager which always executes operations in a
 * sequential manner if there are multiple of them.
 * 
 * @author tranbaoduy
 * 
 */
public class SequentialDatabaseManager extends AbstractDatabaseManager {

    public SequentialDatabaseManager() {
        super();
    }

    @Override
    public void execute(String query, String sourceNodeId,
            String resultTableSchema, List<String> destinationNodeIds)
            throws SQLException, InterruptedException {

        List<String> subQueries = this.executeAndGenerateShipmentQuery(query,
                sourceNodeId, resultTableSchema);

        for (final String subQuery : subQueries) {
            this.execute(subQuery, destinationNodeIds);
        }
    }
    
    @Override
    public void execute(String query, List<String> sourceNodeIds,
            List<String> resultTableSchemata, String destinationNodeId)
            throws SQLException {

        final int num = Math.min(sourceNodeIds.size(),
                resultTableSchemata.size());

        for (int i = 0; i < num; i++) {

            this.execute(query, sourceNodeIds.get(i),
                    resultTableSchemata.get(i), destinationNodeId);
        }
    }

    @Override
    public void execute(String query, List<String> sourceNodeIds,
            String resultTableSchema, String destinationNodeId)
            throws SQLException {

        for (int i = 0; i < sourceNodeIds.size(); i++) {
            this.execute(query, sourceNodeIds.get(i), resultTableSchema,
                    destinationNodeId);
        }
    }

    @Override
    public void execute(String query, List<String> sourceNodeIds,
            List<String> resultTableSchemata, List<String> destinationNodeIds)
            throws SQLException, InterruptedException {

        final int num = Math.min(sourceNodeIds.size(),
                resultTableSchemata.size());

        for (int i = 0; i < num; i++) {

            this.execute(query, sourceNodeIds.get(i),
                    resultTableSchemata.get(i), destinationNodeIds);
        }
    }

    @Override
    public void execute(List<String> queries, String sourceNodeId,
            List<String> resultTableSchemata, List<String> destinationNodeIds)
            throws SQLException {

        final int num = Math.min(
                Math.min(queries.size(), resultTableSchemata.size()),
                destinationNodeIds.size());

        for (int i = 0; i < num; i++) {

            this.execute(queries.get(i), sourceNodeId,
                    resultTableSchemata.get(i), destinationNodeIds.get(i));
        }
    }

    @Override
    public void execute(String query, List<String> nodeIds) throws SQLException {
        if (query.isEmpty()) {
            return;
        }

        for (final String nodeId : nodeIds) {
            this.execute(query, nodeId);
        }
    }

    @Override
    public List<ResultSet> fetch(String query, List<String> nodeIds)
            throws SQLException {

        final List<ResultSet> ret = new ArrayList<ResultSet>();

        for (final String nodeId : nodeIds) {
            ret.add(this.fetch(query, nodeId));
        }

        return ret;
    }
}
