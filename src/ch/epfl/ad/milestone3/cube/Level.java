package ch.epfl.ad.milestone3.cube;

/**
 * Information about a level of a cube dimension. Such information is needed to
 * assign proper column names and sizes to relations holding materialized views.
 * 
 * @author Artyom Stetsenko
 * @author tranbaoduy
 */
public class Level extends NamedEntity {

    /**
     * Placeholder for "ALL"
     */
    public static final String ALL_PLACEHOLDER = "'ALL'";
    
    /**
     * Size of the "ALL" placeholder
     */
    public static final int ALL_PLACEHOLDER_SIZE = 3;

    /**
     * "ALL" string
     */
    public static final String ALL_STRING = "All";
    
    /**
     * The maximum size (number of characters) that a value of this level can
     * have.
     */
    private final int maxValueSize;

    /**
     * Constructor.
     * 
     * @param name
     *            name of the level
     * @param longName
     *            long name of the level
     * @param maxSize
     *            maximum size (number of characters) that a value of this level
     *            can have.
     */
    public Level(String name, String longName, int maxSize) {
        super(name, longName);
        
        // Do not allow fields narrower than "ALL" placeholder
        if (maxSize < Level.ALL_PLACEHOLDER_SIZE) maxSize = Level.ALL_PLACEHOLDER_SIZE;
        this.maxValueSize = maxSize;
    }

    /**
     * Gets maximum size (number of characters) that a value of this level can
     * have.
     * 
     * @return maximum size that a value belonging to this level can have.
     */
    public int getMaxValueSize() {
        return this.maxValueSize;
    }
}
