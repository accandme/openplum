package ch.epfl.data.distribdb.lowlevel;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * An implementation of DatabaseManager which always executes operations in a
 * parallel manner if there are multiple of them.
 * 
 * @author tranbaoduy
 * 
 */
public class ParallelDatabaseManager extends AbstractDatabaseManager {

    private final ExecutorService pool;

    public ParallelDatabaseManager() {
        super();
        this.pool = Executors.newCachedThreadPool();
    }

    @Override
    public void shutDown() {

        super.shutDown();
        this.pool.shutdown();
    }

    @Override
    public void execute(String query, String sourceNodeId,
            String resultTableSchema, List<String> destinationNodeIds)
            throws SQLException, InterruptedException {

        final List<String> subQueries = this.executeAndGenerateShipmentQuery(
                query, sourceNodeId, resultTableSchema);

        final List<Callable<Void>> tasks = new ArrayList<Callable<Void>>();
        final List<SQLException> exceptions = Collections
                .synchronizedList(new ArrayList<SQLException>());

        for (int i = 0; i < destinationNodeIds.size(); i++) {

            final String destinationNodeId = destinationNodeIds.get(i);

            tasks.add(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    try {

                        for (final String subQuery : subQueries) {

                            ParallelDatabaseManager.this.execute(subQuery,
                                    destinationNodeId);
                        }

                    } catch (SQLException e) {
                        exceptions.add(e);
                    }

                    return null;
                }
            });
        }

        this.invoke(tasks, exceptions);
    }

    @Override
    public void execute(final String query, List<String> sourceNodeIds,
            List<String> resultTableSchemata, final String destinationNodeId)
            throws SQLException, InterruptedException {

        final int num = Math.min(sourceNodeIds.size(),
                resultTableSchemata.size());

        final List<Callable<Void>> tasks = new ArrayList<Callable<Void>>();
        final List<SQLException> exceptions = Collections
                .synchronizedList(new ArrayList<SQLException>());

        for (int i = 0; i < num; i++) {

            final String sourceNodeId = sourceNodeIds.get(i);
            final String resultTableSchema = resultTableSchemata.get(i);

            tasks.add(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    try {

                        ParallelDatabaseManager.this.execute(query,
                                sourceNodeId, resultTableSchema,
                                destinationNodeId);

                    } catch (SQLException e) {
                        exceptions.add(e);
                    }

                    return null;
                }
            });
        }

        this.invoke(tasks, exceptions);
    }

    @Override
    public void execute(final String query, List<String> sourceNodeIds,
            final String resultTableSchema, final String destinationNodeId)
            throws SQLException, InterruptedException {

        final List<Callable<Void>> tasks = new ArrayList<Callable<Void>>();
        final List<SQLException> exceptions = Collections
                .synchronizedList(new ArrayList<SQLException>());

        for (int i = 0; i < sourceNodeIds.size(); i++) {

            final String sourceNodeId = sourceNodeIds.get(i);

            tasks.add(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    try {

                        ParallelDatabaseManager.this.execute(query,
                                sourceNodeId, resultTableSchema,
                                destinationNodeId);

                    } catch (SQLException e) {
                        exceptions.add(e);
                    }

                    return null;
                }
            });
        }

        this.invoke(tasks, exceptions);
    }

    @Override
    public void execute(final String query, List<String> sourceNodeIds,
            List<String> resultTableSchemata, List<String> destinationNodeIds)
            throws SQLException, InterruptedException {

        final int num = Math.min(sourceNodeIds.size(),
                resultTableSchemata.size());

        final List<Callable<Void>> tasks = new ArrayList<Callable<Void>>();
        final List<SQLException> exceptions = Collections
                .synchronizedList(new ArrayList<SQLException>());

        for (int i = 0; i < num; i++) {

            final String sourceNodeId = sourceNodeIds.get(i);
            final String resultTableSchema = resultTableSchemata.get(i);
            final String destinationNodeId = destinationNodeIds.get(i);

            tasks.add(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    try {

                        ParallelDatabaseManager.this.execute(query,
                                sourceNodeId, resultTableSchema,
                                destinationNodeId);

                    } catch (SQLException e) {
                        exceptions.add(e);
                    }

                    return null;
                }
            });
        }

        this.invoke(tasks, exceptions);
    }

    @Override
    public void execute(List<String> queries, final String sourceNodeId,
            List<String> resultTableSchemata, List<String> destinationNodeIds)
            throws SQLException, InterruptedException {

        final int num = Math.min(
                Math.min(queries.size(), resultTableSchemata.size()),
                destinationNodeIds.size());

        final List<Callable<Void>> tasks = new ArrayList<Callable<Void>>();
        final List<SQLException> exceptions = Collections
                .synchronizedList(new ArrayList<SQLException>());

        for (int i = 0; i < num; i++) {

            final String query = queries.get(i);
            final String resultTableSchema = resultTableSchemata.get(i);
            final String destinationNodeId = destinationNodeIds.get(i);

            tasks.add(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    try {

                        ParallelDatabaseManager.this.execute(query,
                                sourceNodeId, resultTableSchema,
                                destinationNodeId);

                    } catch (SQLException e) {
                        exceptions.add(e);
                    }

                    return null;
                }
            });
        }

        this.invoke(tasks, exceptions);
    }

    @Override
    public void execute(final String query, final List<String> nodeIds)
            throws SQLException, InterruptedException {

        if (query.isEmpty()) {
            return;
        }

        final List<Callable<Void>> tasks = new ArrayList<Callable<Void>>();
        final List<SQLException> exceptions = Collections
                .synchronizedList(new ArrayList<SQLException>());

        for (final String nodeId : nodeIds) {

            tasks.add(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    try {

                        ParallelDatabaseManager.this.execute(query, nodeId);

                    } catch (SQLException e) {
                        exceptions.add(e);
                    }

                    return null;
                }
            });
        }

        this.invoke(tasks, exceptions);
    }

    @Override
    public List<ResultSet> fetch(final String query, List<String> nodeIds)
            throws SQLException, InterruptedException {

        final List<ResultSet> ret = new ArrayList<ResultSet>();

        for (int i = 0; i < nodeIds.size(); i++) {
            ret.add(null);
        }

        final List<Callable<Void>> tasks = new ArrayList<Callable<Void>>();
        final List<SQLException> exceptions = Collections
                .synchronizedList(new ArrayList<SQLException>());

        for (int i = 0; i < nodeIds.size(); i++) {

            final String nodeId = nodeIds.get(i);
            final int finalIndex = i;

            tasks.add(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    try {

                        ret.set(finalIndex, ParallelDatabaseManager.this.fetch(
                                query, nodeId));

                    } catch (SQLException e) {
                        exceptions.add(e);
                    }

                    return null;
                }
            });
        }

        this.invoke(tasks, exceptions);
        return ret;
    }

    /**
     * Asynchronously invokes all the given tasks in the thread pool and
     * synchronously awaits completion.
     * 
     * @param tasks
     *            Tasks
     * @param exceptions
     *            Exceptions (to be filled by task)
     * 
     * @throws InterruptedException
     * @throws SQLException
     */
    private void invoke(final List<Callable<Void>> tasks,
            final List<SQLException> exceptions) throws InterruptedException,
            SQLException {

        this.pool.invokeAll(tasks);

        if (!exceptions.isEmpty()) {
            throw exceptions.get(0);
        }
    }
}
