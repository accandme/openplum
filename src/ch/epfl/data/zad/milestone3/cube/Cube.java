package ch.epfl.data.zad.milestone3.cube;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import ch.epfl.data.distribdb.lowlevel.DatabaseManager;

/**
 * Logical representation of the data cube which always maintains a current view
 * and provides operators for navigation through different views of it.
 * 
 * @author tranbaoduy
 * @author Amer C
 * 
 */
public class Cube {

    /**
     * Dimensions configuration.
     */
    private Map<String, Dimension> dimensions;

    /**
     * Cube fact fields.
     */
    private List<FactField> factFields;

    /**
     * Database manager.
     */
    private DatabaseManager dbManager;

    /**
     * History of views.
     */
    private List<View> viewHistory;

    /**
     * Index of current view in view history.
     */
    private int curViewIndex;

    /**
     * Repository of all materialized views.
     */
    private Set<View> materializedViewRepo;
    
    /**
     * Arities of levels of all dimensions.
     */
    private Map<String, Integer> arities;

    /**
     * Constructor.
     * 
     * @factTableViewName
     *            Name of the fact table view.
     * @param dims
     *            Dimensions configuration
     * @param factFields
     *            Cube fact fields
     * @param dbManager
     *            Database manager
     * 
     * @throws InterruptedException
     * @throws SQLException
     */
    public Cube(String factTableViewName, Set<Dimension> dims,
            List<FactField> factFields, DatabaseManager dbManager)
            throws SQLException, InterruptedException {

        if (factTableViewName == null || factTableViewName.equals("")) {
            throw new IllegalArgumentException("Fact table view name is null.");
        }

        if (dims == null) {
            throw new IllegalArgumentException("Dimensions list is null.");
        }

        if (factFields == null) {
            throw new IllegalArgumentException("Facts list null.");
        }

        if (dbManager == null) {
            throw new IllegalArgumentException("dbManager is null.");
        }

        this.dimensions = new HashMap<String, Dimension>();

        for (Dimension dim : dims) {
            this.dimensions.put(dim.getName(), dim);
        }

        this.dimensions = Collections.unmodifiableMap(this.dimensions);
        this.factFields = Collections.unmodifiableList(factFields);
        this.dbManager = dbManager;

        this.viewHistory = new ArrayList<View>();
        this.viewHistory.add(new View(this));

        this.materializedViewRepo = new HashSet<View>();
        this.curViewIndex = 0;

        // Add fact table view to repo
        Map<String, Integer> factTableAggState = new HashMap<String, Integer>();
        for (Dimension dim : dims) {
            factTableAggState.put(dim.getName(), dim.getNumLevels() - 1);
        }
        this.materializedViewRepo.add(new View(this, null, factTableAggState,
                factTableViewName, true));
        
        arities = Materializer.getArities(this, "levels_arities", dbManager.getNodeNames().get(0));
    }

    /**
     * Adds a materialized view.
     * 
     * @param view
     *            Materialized view
     */
    public void addMaterializedView(View view) {

        if (view != null) {
            if (view.isMaterialized()) {
                this.materializedViewRepo.add(view);
            }
        }
    }

    
    /**
     * Adds materialized views.
     * 
     * @param views
     *            Materialized views
     */
    public void addMaterializedViews(Set<View> views) {

        if (views != null) {
            for (View view : views) {
                this.addMaterializedView(view);
            }
        }
    }

    /**
     * CUBE OPERATOR: Rolls up on the specified dimension, if possible.
     * 
     * @param dimName
     *            Dimension name
     * 
     * @throws IllegalStateException
     */
    public void rollUp(String dimName) throws IllegalStateException {

        if (!this.dimensions.containsKey(dimName)) {
            throw new IllegalArgumentException("Invalid dimension: " + dimName);
        }

        Dimension dim = this.dimensions.get(dimName);
        View curView = this.getCurView();
        Map<String, Integer> curAggState = curView.getAggState();
        int curLevel = curAggState.get(dimName);

        if (curLevel == 0) {
            throw new IllegalStateException(
                    "Already at top level of dimension: " + dimName);
        }
        
        if(curView.getSliceCriteria().containsKey(dim.getLevel(curLevel).getName())) {
            throw new IllegalStateException("Cannot rollUp: A slice at a deeper level exists");
        }

        Map<String, Integer> newAggState = new HashMap<String, Integer>(
                curAggState);

        newAggState.put(dim.getName(), curLevel - 1);

        this.transitView(new View(this, curView.getSliceCriteria(),
                newAggState));
    }

    /**
     * CUBE OPERATOR: Drills down on the specified dimension, if possible.
     * 
     * @param dimName
     *            Dimension name
     * 
     * @throws IllegalStateException
     */
    public void drillDown(String dimName) throws IllegalStateException {

        if (!this.dimensions.containsKey(dimName)) {
            throw new IllegalArgumentException("Invalid dimName: " + dimName);
        }

        Dimension dim = this.dimensions.get(dimName);
        View curView = this.getCurView();
        Map<String, Integer> curAggState = this.getCurView().getAggState();
        int curLevel = curAggState.get(dimName);

        if (curLevel == dim.getNumLevels() - 1) {
            throw new IllegalStateException(
                    "Already at bottom level of dimension: " + dimName);
        }

        Map<String, Integer> newAggState = new HashMap<String, Integer>(
                curAggState);

        newAggState.put(dim.getName(), curLevel + 1);

        this.transitView(new View(this, curView.getSliceCriteria(),
                newAggState));
    }

    /**
     * CUBE OPERATOR: Adds slice with the specified criterion.
     * 
     * @param levelName
     *            Name of level on which we want to slice
     * @param cond
     *            Condition. Any arbitrary SQL expr referring to
     *            level name by $
     */
    public void addSlice(String levelName, String cond) {
    	Map<String, Integer> aggState = this.getCurView().getAggState();
    	Map<String, String> sliceCriteria = this.getCurView().getSliceCriteria();
    	if(sliceCriteria.containsKey(levelName))
    		throw new IllegalArgumentException("A slice on " + levelName + " already exists!");
    	for(Dimension dim : dimensions.values()) {
    		for(Level lev : dim.getHierarchy())
    			if(levelName.equals(lev.getName()))
    				if(dim.getLevel(levelName) > aggState.get(dim.getName()))
    					throw new IllegalArgumentException("Cannot slice on a deeper level than aggregation!");
    	}
    	Map<String, String> newSliceCriteria = new HashMap<String, String>(sliceCriteria);
    	newSliceCriteria.put(levelName, cond);
        this.transitView(new View(this, newSliceCriteria, aggState));
    }

    /**
     * CUBE OPERATOR: Removes slice on the specified level.
     * 
     * @param levelName
     *            Name of level on which we want to remove slice
     */
    public void removeSlice(String levelName) {
    	Map<String, Integer> aggState = this.getCurView().getAggState();
    	Map<String, String> sliceCriteria = this.getCurView().getSliceCriteria();
    	if(!sliceCriteria.containsKey(levelName))
    		throw new IllegalArgumentException("No slice on " + levelName + " exists!");
    	Map<String, String> newSliceCriteria = new HashMap<String, String>(sliceCriteria);
    	newSliceCriteria.remove(levelName);
        this.transitView(new View(this, newSliceCriteria, aggState));
    }

    /**
     * CUBE OPERATOR: Undoes the previous operation, if possible.
     * 
     * @throws IllegalStateException
     */
    public void undo() throws IllegalStateException {

        if (this.curViewIndex == 0) {
            throw new IllegalStateException("Nothing to undo!");
        }

        this.curViewIndex--;
    }

    /**
     * CUBE OPERATOR: Redoes the previous operation, if possible.
     * 
     * @throws IllegalStateException
     */
    public void redo() throws IllegalStateException {

        if (this.curViewIndex == this.viewHistory.size() - 1) {
            throw new IllegalStateException("Nothing to redo!");
        }

        this.curViewIndex++;
    }

    /**
     * Gets mappings dimension names --> dimensions.
     * 
     * @return Dimensions mapping
     */
    public Map<String, Dimension> getDimensions() {
        return this.dimensions;
    }

    /**
     * Gets fact fields.
     * 
     * @return Fact fields
     */
    public List<FactField> getFactFields() {
        return this.factFields;
    }

    /**
     * Gets the database manager.
     * 
     * @return DB manager
     */
    public DatabaseManager getDatabaseManager() {
        return this.dbManager;
    }

    /**
     * Transits to new view, drops history tail and, if applicable, adds new
     * view to the materialized view repository.
     * 
     * @param index
     *            Index into view history
     */
    private void transitView(View newView) {

        this.curViewIndex++;

        this.viewHistory = this.viewHistory.subList(0, this.curViewIndex);
        this.viewHistory.add(newView);

        if (newView.isMaterialized()) {
            this.materializedViewRepo.add(newView);
        }
    }

    protected View getClosestView(View target) {
        List<View> compatibleViews = new ArrayList<View>();
    	Map<Integer, Long> costMap = new HashMap<Integer, Long>();
    	int i = 0;
        for(View v : this.materializedViewRepo) {
            long cost = 1;
            for(Dimension dim : this.dimensions.values()) {
            	int srcLev = v.getAggState().get(dim.getName());
            	int dstLev = target.getAggState().get(dim.getName());
                if(srcLev < dstLev) {
                	cost = -1;
                    break;
                }
                if(srcLev != 0)
                	cost *= new Long(this.arities.get(dim.getLevel(srcLev).getName()));
                if(dstLev != 0)
                	cost /= new Long(this.arities.get(dim.getLevel(dstLev).getName()));
            }
            if(cost != -1) {
                compatibleViews.add(v);
                costMap.put(i++, cost);
            }
        }
        List<Entry<Integer, Long>> costMapEntries = new LinkedList<Entry<Integer, Long>>(costMap.entrySet());
        Collections.sort(costMapEntries, new Comparator<Entry<Integer, Long>>() {
			public int compare(Entry<Integer, Long> o1, Entry<Integer, Long> o2) {
				if(o1.getValue() > o2.getValue())
					return 1;
				if(o1.getValue() < o2.getValue())
					return -1;
				return 0;
			}
		});
        if(costMapEntries.size() > 0)
            return compatibleViews.get(costMapEntries.get(0).getKey());
        throw new IllegalStateException("No compatible views in repo");
    }

    /**
     * Gets current view.
     * 
     * @return current view
     */
    public View getCurView() {
        return this.viewHistory.get(this.curViewIndex);
    }

    /**
     * Retrieve data of the current view.
     * 
     * @return SQL result set of current view
     * 
     * @throws InterruptedException
     * @throws SQLException
     */
    public ResultSet getCurViewData() throws SQLException, InterruptedException {

        return this.getCurView().getData();
    }
}
