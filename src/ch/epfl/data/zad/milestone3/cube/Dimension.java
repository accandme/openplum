package ch.epfl.data.zad.milestone3.cube;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Multilevel dimension of the data cube. Each dimension is defined by a name
 * (corresponding to the dimension table name) and a hierarchy of fields
 * (corresponding to fields in the dimension table).
 * <p>
 * Level 0 denotes the full dimension (GROUP-BY no fields in the hierarchy,
 * represented by the dimension name). Levels 1, 2, ..., N denote GROUP-BY 1st,
 * 2nd, ..., Nth field in the hierarchy.
 * <p>
 * Object of this class is generally configuration-immutable.
 */
public class Dimension extends NamedEntity {

    /**
     * Field names in hierarchy.
     */
    private final List<Level> hierarchy;

    /**
     * Invalid level index.
     */
    public static final int INVALID_LEVEL = -1;

    /**
     * Constructor.
     * 
     * @param name
     *            Dimension name
     * @param longName
     *            Long, descriptive name of the dimension
     * @param keyColumnName
     *            Name of the fact table column referencing this dimension
     * @param hierarchy
     *            Hierarchy of fields
     */
    public Dimension(String name, String longName, List<Level> hierarchy) {

        super(name, longName);

        if (hierarchy == null) {
            throw new IllegalArgumentException("Dimension hierarchy is null.");
        }

        final Set<Level> testSet = new HashSet<Level>(hierarchy);
        if (testSet.size() < hierarchy.size()) {
            throw new IllegalArgumentException("Duplicates in hierarchy: "
                    + hierarchy);
        }

        if (testSet.contains(name)) {
            throw new IllegalArgumentException("Hierarchy has dimension name: "
                    + name);
        }

        this.hierarchy = Collections.unmodifiableList(hierarchy);
    }

    /**
     * Constructor without the long name.
     * 
     * @param name
     *            Dimension name
     * @param keyColumnName
     *            Name of the fact table column referencing this dimension
     * @param hierarchy
     *            Hierarchy of fields
     */
    public Dimension(String name, List<Level> hierarchy) {
        this(name, null, hierarchy);
    }

    /**
     * Gets field hierarchy
     * 
     * @return Field hierarchy
     */
    public List<Level> getHierarchy() {
        return this.hierarchy;
    }

    /**
     * Gets total no. of levels (including top one)
     * 
     * @return Number of levels (>= 1)
     */
    public int getNumLevels() {
        return this.hierarchy.size() + 1;
    }

    /**
     * Gets level at specified index
     * 
     * @param levelIndex
     *            Level index
     * @return level at specified index
     */
    public Level getLevel(int levelIndex) {

        if (levelIndex < 0 || levelIndex > this.hierarchy.size()) {
            throw new IllegalArgumentException("Invalid level: " + levelIndex);
        }

        if (levelIndex == 0) {
            return new Level(this.getName(), this.getLongName(),
                    Level.ALL_PLACEHOLDER_SIZE);
        }

        return this.hierarchy.get(levelIndex - 1);
    }

    /**
     * Gets name (field/dimension) at the specified level
     * 
     * @param level
     *            Level index
     * 
     * @return Name
     */
    public String getName(int level) {
        return this.getLevel(level).getName();
    }

    public int getSize(int level) {
        return this.getLevel(level).getMaxValueSize();
    }

    /**
     * Get level of the specified field/dimension name.
     * 
     * @param name
     *            Field/dimension name
     * 
     * @return Field level, INVALID_LEVEL if invalid
     */
    public int getLevel(String name) {

        if (name.equalsIgnoreCase(this.getName())) {
            return 0;
        }

        for (int i = 0; i < this.hierarchy.size(); i++) {
            if (this.hierarchy.get(i).getName().equalsIgnoreCase(name)) {
                return i + 1;
            }
        }

        return INVALID_LEVEL;
    }
}
