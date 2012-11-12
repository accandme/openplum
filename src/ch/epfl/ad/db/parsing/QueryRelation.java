package ch.epfl.ad.db.parsing;

import java.util.Arrays;
import java.util.List;

public class QueryRelation extends Relation {
	
	private List<Field> fields;
	private List<Relation> relations;
	private List<Qualifier> qualifiers;
	
	public QueryRelation(Field field, Relation relation) {
		this(field, Arrays.asList(relation));
	}
	
	public QueryRelation(Field field, List<Relation> relations) {
		this(Arrays.asList(field), relations);
	}
	
	public QueryRelation(Field field, Relation relation, Qualifier qualifier) {
		this(field, relation, Arrays.asList(qualifier));
	}
	
	public QueryRelation(Field field, Relation relation, List<Qualifier> qualifiers) {
		this(field, Arrays.asList(relation), qualifiers);
	}
	
	public QueryRelation(Field field, List<Relation> relations, Qualifier qualifier) {
		this(field, relations, Arrays.asList(qualifier));
	}
	
	public QueryRelation(Field field, List<Relation> relations, List<Qualifier> qualifiers) {
		this(Arrays.asList(field), relations, qualifiers);
	}
	
	public QueryRelation(List<Field> fields, Relation relation) {
		this(fields, Arrays.asList(relation));
	}
	
	public QueryRelation(List<Field> fields, List<Relation> relations) {
		this(fields, relations, (List<Qualifier>)null);
	}
	
	public QueryRelation(List<Field> fields, Relation relation, Qualifier qualifier) {
		this(fields, relation, Arrays.asList(qualifier));
	}
	
	public QueryRelation(List<Field> fields, Relation relation, List<Qualifier> qualifiers) {
		this(fields, Arrays.asList(relation), qualifiers);
	}
	
	public QueryRelation(List<Field> fields, List<Relation> relations, Qualifier qualifier) {
		this(fields, relations, Arrays.asList(qualifier));
	}
	
	public QueryRelation(List<Field> fields, List<Relation> relations, List<Qualifier> qualifiers) {
		this(fields, relations, qualifiers, null);
	}
	
	public QueryRelation(Field field, Relation relation, String alias) {
		this(field, relation, (List<Qualifier>)null, alias);
	}
	
	public QueryRelation(Field field, List<Relation> relations, String alias) {
		this(field, relations, (List<Qualifier>)null, alias);
	}
	
	public QueryRelation(List<Field> fields, Relation relation, String alias) {
		this(fields, relation, (List<Qualifier>)null, alias);
	}
	
	public QueryRelation(List<Field> fields, List<Relation> relations, String alias) {
		this(fields, relations, (List<Qualifier>)null, alias);
	}
	
	public QueryRelation(Field field, Relation relation, Qualifier qualifier, String alias) {
		this(field, relation, Arrays.asList(qualifier), alias);
	}
	
	public QueryRelation(Field field, Relation relation, List<Qualifier> qualifiers, String alias) {
		this(field, Arrays.asList(relation), qualifiers, alias);
	}
	
	public QueryRelation(Field field, List<Relation> relations, Qualifier qualifier, String alias) {
		this(field, relations, Arrays.asList(qualifier), alias);
	}
	
	public QueryRelation(Field field, List<Relation> relations, List<Qualifier> qualifiers, String alias) {
		this(Arrays.asList(field), relations, qualifiers, alias);
	}
	
	public QueryRelation(List<Field> fields, Relation relation, Qualifier qualifier, String alias) {
		this(fields, relation, Arrays.asList(qualifier), alias);
	}
	
	public QueryRelation(List<Field> fields, Relation relation, List<Qualifier> qualifiers, String alias) {
		this(fields, Arrays.asList(relation), qualifiers, alias);
	}
	
	public QueryRelation(List<Field> fields, List<Relation> relations, Qualifier qualifier, String alias) {
		this(fields, relations, Arrays.asList(qualifier), alias);
	}

	public QueryRelation(List<Field> fields, List<Relation> relations, List<Qualifier> qualifiers, String alias) {
		if (fields == null) {
			throw new IllegalArgumentException("Query relation fields cannot be null.");
		}
		this.fields = fields;
		this.relations = relations;
		this.qualifiers = qualifiers;
		this.alias = alias;
	}
	
	public List<Field> getFields() {
		return this.fields;
	}
	
	public List<Relation> getRelations() {
		return this.relations;
	}
	
	public List<Qualifier> getQualifiers() {
		return this.qualifiers;
	}
}
