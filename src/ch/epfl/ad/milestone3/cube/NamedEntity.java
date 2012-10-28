package ch.epfl.ad.milestone3.cube;

/**
 * Common and super-simple implementation of a "named" DB entity (e.g.
 * dimension, hierarchy level, fact field), encapsulating a name (for operation)
 * and a long name (for beautiful output display).
 * <p>
 * NB. Identity (via equals() and hashcode()) is by "name" only.
 * 
 * @author tranbaoduy
 * 
 */
public abstract class NamedEntity {

    /**
     * Name of the entity.
     */
    private final String name;

    /**
     * Long (descriptive) name of the entity, e.g. qty => Quantity. Needed for
     * beautiful output display only.
     */
    private final String longName;

    /**
     * Constructor.
     * 
     * @param name
     *            name of the entity
     */
    public NamedEntity(String name) {
        this(name, name);
    }

    /**
     * Constructor.
     * 
     * @param name
     *            short name of the entity
     * @param longName
     *            long, descriptive name of the entity
     */
    public NamedEntity(String name, String longName) {

        if (name == null) {
            throw new IllegalArgumentException("name is null");
        }

        if (longName == null) {
            throw new IllegalArgumentException("longName is null");
        }

        this.name = name;
        this.longName = longName;
    }

    /**
     * Gets the name of the entity
     * 
     * @return (short) name of the entity
     */
    public String getName() {
        return this.name;
    }

    /**
     * Gets the long, descriptive name of the entity
     * 
     * @return long name of the entity
     */
    public String getLongName() {
        return this.longName;
    }

    @Override
    public int hashCode() {

        int hash = 1;
        hash = hash * 31 + this.name.hashCode();
        return hash;
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof NamedEntity)) {
            return false;
        }

        final NamedEntity that = (NamedEntity) obj;
        return this.name.equals(that.name);
    }
}
