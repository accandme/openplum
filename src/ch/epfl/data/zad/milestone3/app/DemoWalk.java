package ch.epfl.data.zad.milestone3.app;

import java.sql.SQLException;
import java.util.Set;

import ch.epfl.data.distribdb.app.AbstractQuery;
import ch.epfl.data.distribdb.lowlevel.DatabaseManager;
import ch.epfl.data.zad.milestone3.cube.Cube;
import ch.epfl.data.zad.milestone3.cube.Dimension;
import ch.epfl.data.zad.milestone3.cube.Materializer;
import ch.epfl.data.zad.milestone3.cube.ViewPrinter;

public class DemoWalk extends AbstractQuery {

    @Override
    public void run(String[] args) throws SQLException, InterruptedException {
        DatabaseManager dbManager = createDatabaseManager(args[0]);
        dbManager.setResultShipmentBatchSize(5000);

        Set<Dimension> dimensions = CubeSettings.getDimensions();
        Cube salesCube = new Cube("view_fact_sales", dimensions,
                CubeSettings.getFactFields(), dbManager);

        salesCube.addMaterializedViews(Materializer.getPreloadedViews(salesCube,
                "materialized_views_catalog", dbManager.getNodeNames().get(0)));

        // All these can be called in any order
        ViewPrinter.printView(salesCube.getCurView());

        salesCube.drillDown("dim_time");
        ViewPrinter.printView(salesCube.getCurView());

        salesCube.addSlice("time_year", "$ BETWEEN 1995 AND 1997");
        ViewPrinter.printView(salesCube.getCurView());

        salesCube.drillDown("dim_time");
        ViewPrinter.printView(salesCube.getCurView());

        salesCube.undo();
        ViewPrinter.printView(salesCube.getCurView());

        salesCube.redo();
        ViewPrinter.printView(salesCube.getCurView());

        dbManager.shutDown();
    }

    public static void main(String[] args) throws SQLException,
            InterruptedException {
        new DemoWalk().run(args);
    }
}
