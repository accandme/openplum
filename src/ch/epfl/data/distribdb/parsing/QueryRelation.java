package ch.epfl.data.distribdb.parsing;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * An SQL query (any query is a relation).
 * 
 * @author Artyom Stetsenko
 */
public class QueryRelation extends Relation {
	
	/**
	 * The fields of this query's SELECT clause.
	 */
	private List<Field> fields;
	
	/**
	 * The relations of this query's FROM clause.
	 */
	private List<Relation> relations;
	
	/**
	 * The qualifiers of this query's WHERE clause (connected by conjunction).
	 */
	private List<Qualifier> qualifiers;
	
	/**
	 * The fields of this query's GROUP BY clause. 
	 */
	private List<Field> grouping;
	
	/**
	 * The qualifiers of this query's HAVING clause. 
	 */
	private List<Qualifier> groupingQualifiers;
	
	/**
	 * The fields of this query's ORDER BY clause. 
	 */
	private List<OrderingItem> ordering;
	
	/**
	 * Constructor of a query.
	 * 
	 * @param field
	 *                the field of this query's SELECT clause
	 * @param relation
	 *                the relation of this query's FROM clause
	 */
	public QueryRelation(Field field, Relation relation) {
		this(field, new LinkedList<Relation>(Arrays.asList(relation)));
	}
	
	/**
	 * Constructor of a query.
	 * 
	 * @param field
	 *                the field of this query's SELECT clause
	 * @param relations
	 *                the relations of this query's FROM clause
	 */
	public QueryRelation(Field field, List<Relation> relations) {
		this(new LinkedList<Field>(Arrays.asList(field)), relations);
	}
	
	/**
	 * Constructor of a query.
	 * 
	 * @param fields
	 *                the fields of this query's SELECT clause
	 * @param relation
	 *                the relation of this query's FROM clause
	 */
	public QueryRelation(List<Field> fields, Relation relation) {
		this(fields, new LinkedList<Relation>(Arrays.asList(relation)));
	}

	/**
	 * Constructor of a query.
	 * 
	 * @param fields
	 *                the fields of this query's SELECT clause
	 * @param relations
	 *                the relations of this query's FROM clause
	 */
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
	
	/**
	 * Getter of this query's SELECT clause fields.
	 * 
	 * @return list of this query's SELECT clause fields
	 */
	public List<Field> getFields() {
		return this.fields;
	}
	
	/**
	 * Getter of this query's FROM clause relations.
	 * 
	 * @return list of this query's FROM clause relations
	 */
	public List<Relation> getRelations() {
		return this.relations;
	}
	
	/**
	 * Setter of this query's WHERE clause qualifier.
	 * 
	 * @param qualifier
	 *                this query's WHERE clause qualifier
	 * @return this query
	 */
	public QueryRelation setQualifier(Qualifier qualifier) {
		return this.setQualifiers(new LinkedList<Qualifier>(Arrays.asList(qualifier)));
	}
	
	/**
	 * Setter of this query's WHERE clause qualifiers.
	 * 
	 * @param qualifiers
	 *                this query's WHERE clause qualifiers
	 * @return this query
	 */
	public QueryRelation setQualifiers(List<Qualifier> qualifiers) {
		this.qualifiers = qualifiers;
		return this;
	}
	
	/**
	 * Getter of this query's WHERE clause qualifiers.
	 * 
	 * @return list of this query's WHERE clause qualifiers (connected by conjunction)
	 */
	public List<Qualifier> getQualifiers() {
		return this.qualifiers;
	}
	
	/**
	 * Setter of this query's GROUP BY clause field.
	 * 
	 * @param grouping
	 *                this query's GROUP BY clause field
	 * @return this query
	 */
	public QueryRelation setGrouping(Field grouping) {
		return this.setGrouping(new LinkedList<Field>(Arrays.asList(grouping)));
	}
	
	/**
	 * Setter of this query's GROUP BY clause fields.
	 * 
	 * @param grouping
	 *                this query's GROUP BY clause fields
	 * @return this query
	 */
	public QueryRelation setGrouping(List<Field> grouping) {
		this.grouping = grouping;
		return this;
	}
	
	/**
	 * Getter of this query's GROUP BY clause fields.
	 * 
	 * @return list of this query's GROUP BY clause fields
	 */
	public List<Field> getGrouping() {
		return this.grouping;
	}
	
	/**
	 * Setter of this query's HAVING clause qualifier.
	 * 
	 * @param qualifier
	 *                this query's HAVING clause qualifier
	 * @return this query
	 */
	public QueryRelation setGroupingQualifier(Qualifier qualifier) {
		return this.setGroupingQualifiers(new LinkedList<Qualifier>(Arrays.asList(qualifier)));
	}
	
	/**
	 * Setter of this query's HAVING clause qualifiers.
	 * 
	 * @param qualifiers
	 *                this query's HAVING clause qualifiers
	 * @return this query
	 */
	public QueryRelation setGroupingQualifiers(List<Qualifier> qualifiers) {
		this.groupingQualifiers = qualifiers;
		return this;
	}
	
	/**
	 * Getter of this query's HAVING clause qualifiers.
	 * 
	 * @return list of this query's HAVING clause qualifiers
	 */
	public List<Qualifier> getGroupingQualifiers() {
		return this.groupingQualifiers;
	}
	
	/**
	 * Setter of this query's ORDER BY clause field.
	 * 
	 * @param ordering
	 *                this query's ORDER BY clause field
	 * @return this query
	 */
	public QueryRelation setOrdering(OrderingItem ordering) {
		return this.setOrdering(new LinkedList<OrderingItem>(Arrays.asList(ordering)));
	}
	
	/**
	 * Setter of this query's ORDER BY clause fields.
	 * 
	 * @param ordering
	 *                this query's ORDER BY clause fields
	 * @return this query
	 */
	public QueryRelation setOrdering(List<OrderingItem> ordering) {
		this.ordering = ordering;
		return this;
	}
	
	/**
	 * Getter of this query's ORDER BY clause fields.
	 * 
	 * @return list of this query's ORDER BY clause fields
	 */
	public List<OrderingItem> getOrdering() {
		return this.ordering;
	}
	
	@Override
	public QueryRelation setAlias(String alias) {
		this.alias = alias;
		return this;
	}
	
	/**
	 * Retrieves whether this query is aggregate.
	 * 
	 * @return true if this query has a GROUP BY clause, a HAVING clause, or any aggregate
	 *         fields in the SELECT clause; false otherwise
	 */
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
	
	/**
	 * Replaces a relation in this query with a new named relation.
	 * 
	 * @param oldRelation
	 *                relation to replace
	 * @param newRelation
	 *                relation to replace with
	 */
	public void replaceRelation(Relation oldRelation, NamedRelation newRelation) {
		if (!this.tryAndReplaceRelation(oldRelation, newRelation)) { // this really shouldn't happen
			throw new IllegalArgumentException("Could not find relation " + oldRelation + " to replace with " + newRelation + ".");
		}
	}
	
	private boolean tryAndReplaceRelation(Relation oldRelation, NamedRelation newRelation) {
		
		// iterate through relations (FROM clause)
		for (Iterator<Relation> it = this.relations.iterator(); it.hasNext(); ) {
			Relation relation = it.next();
			if (relation == oldRelation) { // reference equality
				it.remove();
				this.replaceFieldRelations(oldRelation, newRelation); // fix old field references
				this.relations.add(newRelation);
				return true;
			} else if (relation instanceof QueryRelation) {
				if (((QueryRelation)relation).tryAndReplaceRelation(oldRelation, newRelation)) {
					return true;
				}
			}
		}
		
		// iterate through qualifiers (WHERE clause)
		if (this.qualifiers != null) {
			for (Qualifier qualifier : this.qualifiers) {
				int operandIndex = -1; // operand position in list sometimes matters, e.g. for BETWEEN
				for (Iterator<Operand> it = qualifier.getOperands().iterator(); it.hasNext(); ) {
					Operand operand = it.next();
					operandIndex++;
					if (operand == oldRelation) { // reference equality
						it.remove();
						// TODO make sure that "*" works here
						// comment: should work
						qualifier.getOperands().add(operandIndex, new QueryRelation(new NamedField(newRelation, "*"), newRelation));
						return true;
					} else {
						 if (operand instanceof QueryRelation) {
							 if (((QueryRelation)operand).tryAndReplaceRelation(oldRelation, newRelation)) {
								 return true;
							 }
						 }
					}
				}
			}
		}
		
		// iterate through grouping qualifiers (HAVING clause)
		if (this.groupingQualifiers != null) {
			for (Qualifier qualifier : this.groupingQualifiers) {
				int operandIndex = -1;
				for (Iterator<Operand> it = qualifier.getOperands().iterator(); it.hasNext(); ) {
					Operand operand = it.next();
					if (operand == oldRelation) {
						it.remove();
						qualifier.getOperands().add(operandIndex, new QueryRelation(new NamedField(newRelation, "*"), newRelation));
						return true;
					} else {
						 if (operand instanceof QueryRelation) {
							 if (((QueryRelation)operand).tryAndReplaceRelation(oldRelation, newRelation)) {
								 return true;
							 }
						 }
					}
				}
			}
		}
		
		return false;
	}
	
	private void replaceFieldRelations(Relation oldRelation, Relation newRelation) {
		for (Field field : this.fields) {
			this.replaceFieldRelation(field, oldRelation, newRelation);
		}
		if (this.qualifiers != null) {
			for (Qualifier qualifier : this.qualifiers) {
				for (Operand operand : qualifier.getOperands()) {
					if (operand instanceof Field) {
						this.replaceFieldRelation((Field)operand, oldRelation, newRelation);
					} else if (operand instanceof QueryRelation) {
						((QueryRelation)operand).replaceFieldRelations(oldRelation, newRelation);
					}
				}
			}
		}
		if (this.grouping != null) {
			for (Field groupingField : this.grouping) {
				this.replaceFieldRelation(groupingField, oldRelation, newRelation);
			}
		}
		if (this.groupingQualifiers != null) {
			for (Qualifier groupingQualifier : this.groupingQualifiers) {
				for (Operand operand : groupingQualifier.getOperands()) {
					if (operand instanceof Field) {
						this.replaceFieldRelation((Field)operand, oldRelation, newRelation);
					} else if (operand instanceof QueryRelation) {
						((QueryRelation)operand).replaceFieldRelations(oldRelation, newRelation);
					}
				}
			}
		}
		if (this.ordering != null) {
			for (OrderingItem orderingItem : this.ordering) {
				this.replaceFieldRelation(orderingItem.getField(), oldRelation, newRelation);
			}
		}
	}
	
	private void replaceFieldRelation(Field field, Relation oldRelation, Relation newRelation) {
		if (field instanceof NamedField) {
			if (((NamedField)field).getRelation() == oldRelation) {
				((NamedField)field).replaceRelation(newRelation);
			}
		} else if (field instanceof AggregateField) {
			this.replaceFieldRelation(((AggregateField)field).getField(), oldRelation, newRelation);
		} else if (field instanceof FunctionField) {
			this.replaceFieldRelation(((FunctionField)field).getField(), oldRelation, newRelation);
		} else if (field instanceof ExpressionField) {
			for (Field subField : ((ExpressionField)field).getFields()) {
				this.replaceFieldRelation(subField, oldRelation, newRelation);
			}
		}
	}
	
	/**
	 * Retrieves the string representation of this query, without including the alias.
	 * 
	 * @return the query string of this query without its alias
	 */
	public String toUnaliasedString() {
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
			string.append(relation.toString());
			prefix = ", ";
		}
		if (this.qualifiers != null) {
			string.append(" WHERE ");
			prefix = "";
			for (Qualifier qualifier : this.qualifiers) {
				string.append(prefix);
				string.append(qualifier.toString());
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
				string.append(groupingQualifier.toString());
				prefix = " AND ";
			}
		}
		if (this.ordering != null) {
			string.append(" ORDER BY ");
			prefix = "";
			for (OrderingItem field : this.ordering) {
				string.append(prefix);
				string.append(field.toString());
				prefix = ", ";
			}
		}
		return string.toString();
	}
	
	@Override
	public String toString() {
		return this.alias != null ? String.format("(%s) %s", this.toUnaliasedString(), this.alias) : this.toUnaliasedString();
	}
	
	/**
	 * Retrieves the intermediate string representation of this query. Intermediate queries are used
	 * when an aggregate query is run on the worker nodes (i.e. the first step of execution of this query,
	 * with the intent of running the final query afterwards).
	 * 
	 * @return the intermediate query string of this query
	 */
	public String toIntermediateString() {
		StringBuilder string = new StringBuilder("SELECT ");
		List<Field> intermediateFields = new LinkedList<Field>();
		intermediateFields.addAll(this.fields);
		if (this.grouping != null) {
			for (Field field : this.grouping) {
				if (!intermediateFields.contains(field)) { // assuming reference equality
					intermediateFields.add(field);
				}
			}
		}
		if (this.groupingQualifiers != null) {
			for (Qualifier groupingQualifier : this.groupingQualifiers) {
				for (Operand operand : groupingQualifier.getOperands()) {
					if (operand instanceof Field && !intermediateFields.contains((Field)operand)) { // assuming reference equality
						intermediateFields.add((Field)operand);
					}
				}
			}
		}
		if (this.ordering != null) {
			for (OrderingItem orderingField : this.ordering) {
				if (!intermediateFields.contains(orderingField.getField())) {
					intermediateFields.add(orderingField.getField());
				}
			}
		}
		String prefix = "";
		int i = 0;
		for (Field field : intermediateFields) {
			String fieldString = field.toFullIntermediateString(++i);
			if (!fieldString.isEmpty()) {
				string.append(prefix);
				string.append(fieldString);
				prefix = ", ";
			}
		}
		boolean isAggregate = this.isAggregate();
		string.append(" FROM ");
		prefix = "";
		for (Relation relation : this.relations) {
			if (isAggregate && relation instanceof QueryRelation && ((QueryRelation)relation).isAggregate()) {
				throw new IllegalStateException("Cannot convert a query with a nested aggregate query (in FROM) to intermediate string.");
			}
			string.append(prefix);
			string.append(relation.toString());
			prefix = ", ";
		}
		if (this.qualifiers != null) {
			string.append(" WHERE ");
			prefix = "";
			for (Qualifier qualifier : this.qualifiers) {
				for (Operand operand : qualifier.getOperands()) {
					if (isAggregate && operand instanceof QueryRelation && ((QueryRelation)operand).isAggregate()) {
						throw new IllegalStateException("Cannot convert a query with a nested aggregate query (in WHERE) to intermediate string.");
					}
				}
				string.append(prefix);
				string.append(qualifier.toString());
				prefix = " AND ";
			}
		}
		if (this.grouping != null) {
			string.append(" GROUP BY ");
			prefix = "";
			for (Field field : this.grouping){
				string.append(prefix);
				i = intermediateFields.indexOf(field) + 1;
				string.append(field.toAliasedIntermediateString(i));
				prefix = ", ";
			}
		}
		if (this.groupingQualifiers != null) { // ensure no nested aggregate queries in HAVING
			for (Qualifier groupingQualifier : this.groupingQualifiers) {
				for (Operand operand : groupingQualifier.getOperands()) {
					if (isAggregate && operand instanceof QueryRelation && ((QueryRelation)operand).isAggregate()) {
						throw new IllegalStateException("Cannot convert a query with a nested aggregate query (in HAVING) to intermediate string.");
					}
				}
			}
		}
		return string.toString();
	}
	
	/**
	 * Retrieves the final string representation of this query. Final queries are used when
	 * an aggregate query is run on the master node to merge intermediate aggregate results
	 * from the worker nodes (i.e. the second and final step of execution
	 * of this query).
	 * 
	 * @param intermediateRelation
	 *                the named relation holding the intermediate results from worker nodes
	 * @return the final query string of this query
	 */
	public String toFinalString(NamedRelation intermediateRelation) {
		StringBuilder string = new StringBuilder("SELECT ");
		List<Field> intermediateFields = new LinkedList<Field>();
		intermediateFields.addAll(this.fields);
		if (this.grouping != null) {
			for (Field field : this.grouping) {
				if (!intermediateFields.contains(field)) { // assuming reference equality
					intermediateFields.add(field);
				}
			}
		}
		if (this.groupingQualifiers != null) {
			for (Qualifier groupingQualifier : this.groupingQualifiers) {
				for (Operand operand : groupingQualifier.getOperands()) {
					if (operand instanceof Field && !intermediateFields.contains((Field)operand)) { // assuming reference equality
						intermediateFields.add((Field)operand);
					}
				}
			}
		}
		if (this.ordering != null) {
			for (OrderingItem orderingField : this.ordering) {
				if (!intermediateFields.contains(orderingField.getField())) {
					intermediateFields.add(orderingField.getField());
				}
			}
		}
		String prefix = "";
		for (Field field : this.fields) {
			string.append(prefix);
			string.append(field.toFullFinalString(intermediateRelation, intermediateFields.indexOf(field) + 1));
			prefix = ", ";
		}
		string.append(" FROM ");
		string.append(intermediateRelation.toString());
		if (this.grouping != null) {
			string.append(" GROUP BY ");
			prefix = "";
			for (Field field : this.grouping){
				string.append(prefix);
				string.append(field.toAliasedFinalString(intermediateRelation, intermediateFields.indexOf(field) + 1));
				prefix = ", ";
			}
		}
		if (this.groupingQualifiers != null) {
			string.append(" HAVING ");
			prefix = "";
			for (Qualifier groupingQualifier : this.groupingQualifiers) {
				string.append(prefix);
				string.append(groupingQualifier.toFinalString(intermediateRelation, intermediateFields));
				prefix = " AND ";
			}
		}
		if (this.ordering != null) {
			string.append(" ORDER BY ");
			prefix = "";
			for (OrderingItem field : this.ordering) {
				string.append(prefix);
				string.append(field.toFinalString(intermediateRelation, intermediateFields.indexOf(field.getField()) + 1));
				prefix = ", ";
			}
		}
		return string.toString();
	}
}
