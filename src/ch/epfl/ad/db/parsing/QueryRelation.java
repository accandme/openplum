package ch.epfl.ad.db.parsing;

import java.util.Arrays;
import java.util.List;

public class QueryRelation extends Relation {
	
	private List<Field> fields;
	private List<Relation> relations;
	private List<Qualifier> qualifiers;
	private List<Field> grouping;
	private List<Qualifier> groupingQualifiers;
	private List<OrderingItem> ordering;
	
	public QueryRelation(Field field, Relation relation) {
		this(field, Arrays.asList(relation));
	}
	
	public QueryRelation(Field field, List<Relation> relations) {
		this(Arrays.asList(field), relations);
	}
	
	public QueryRelation(List<Field> fields, Relation relation) {
		this(fields, Arrays.asList(relation));
	}

	public QueryRelation(List<Field> fields, List<Relation> relations) {
		if (fields == null) {
			throw new IllegalArgumentException("Query relation fields cannot be null.");
		}
		if (relations == null) {
			throw new IllegalArgumentException("Query relation relations cannot be null.");
		}
		this.fields = fields;
		this.relations = relations;
	}
	
	public List<Field> getFields() {
		return this.fields;
	}
	
	public List<Relation> getRelations() {
		return this.relations;
	}
	
	public QueryRelation setQualifier(Qualifier qualifier) {
		return this.setQualifiers(Arrays.asList(qualifier));
	}
	
	public QueryRelation setQualifiers(List<Qualifier> qualifiers) {
		this.qualifiers = qualifiers;
		return this;
	}
	
	public List<Qualifier> getQualifiers() {
		return this.qualifiers;
	}
	
	public QueryRelation setGrouping(Field grouping) {
		return this.setGrouping(Arrays.asList(grouping));
	}
	
	public QueryRelation setGrouping(List<Field> grouping) {
		this.grouping = grouping;
		return this;
	}
	
	public List<Field> getGrouping() {
		return this.grouping;
	}
	
	public QueryRelation setGroupingQualifier(Qualifier qualifier) {
		return this.setGroupingQualifiers(Arrays.asList(qualifier));
	}
	
	public QueryRelation setGroupingQualifiers(List<Qualifier> qualifier) {
		this.groupingQualifiers = qualifier;
		return this;
	}
	
	public List<Qualifier> getGroupingQualifiers() {
		return this.groupingQualifiers;
	}
	
	public QueryRelation setOrdering(OrderingItem ordering) {
		return this.setOrdering(Arrays.asList(ordering));
	}
	
	public QueryRelation setOrdering(List<OrderingItem> ordering) {
		this.ordering = ordering;
		return this;
	}
	
	public List<OrderingItem> getOrdering() {
		return this.ordering;
	}
	
	@Override
	public QueryRelation setAlias(String alias) {
		this.alias = alias;
		return this;
	}
	
	public boolean isAggregate() {
		if (this.grouping == null && this.groupingQualifiers == null) {
			for (Field field : this.fields) {
				if (field.isAggregate()) {
					return true;
				}
			}
			return false;
		}
		return true;
	}
	
	@Override
	public String toString() {
		StringBuilder string = new StringBuilder("SELECT ");
		String prefix = "";
		for (Field field : this.fields) {
			string.append(prefix);
			string.append(field.toFullString());
			prefix = ", ";
		}
		string.append(" FROM ");
		prefix = "";
		for (Relation relation : this.relations) {
			string.append(prefix);
			string.append(relation);
			prefix = ", ";
		}
		if (this.qualifiers != null) {
			string.append(" WHERE ");
			prefix = "";
			for (Qualifier qualifier : this.qualifiers) {
				string.append(prefix);
				string.append(qualifier);
				prefix = " AND ";
			}
		}
		if (this.grouping != null) {
			string.append(" GROUP BY ");
			prefix = "";
			for (Field field : this.grouping){
				string.append(prefix);
				string.append(field.toAliasedString());
				prefix = ", ";
			}
		}
		if (this.groupingQualifiers != null) {
			string.append(" HAVING ");
			prefix = "";
			for (Qualifier groupingQualifier : this.groupingQualifiers) {
				string.append(prefix);
				string.append(groupingQualifier);
				prefix = " AND ";
			}
		}
		if (this.ordering != null) {
			string.append(" ORDER BY ");
			prefix = "";
			for (OrderingItem field : this.ordering) {
				string.append(prefix);
				string.append(field);
				prefix = ", ";
			}
		}
		return this.alias != null ? String.format("(%s) %s", string, this.alias) : string.toString();
	}
}
