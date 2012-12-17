package ch.epfl.ad.db.parsing;

import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.EExpressionType;
import gudusoft.gsqlparser.ESqlStatementType;
import gudusoft.gsqlparser.TBaseType;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.nodes.TExpression;
import gudusoft.gsqlparser.nodes.TExpressionList;
import gudusoft.gsqlparser.nodes.TFunctionCall;
import gudusoft.gsqlparser.nodes.TGroupByItem;
import gudusoft.gsqlparser.nodes.TGroupByItemList;
import gudusoft.gsqlparser.nodes.TJoin;
import gudusoft.gsqlparser.nodes.TResultColumn;
import gudusoft.gsqlparser.nodes.TTable;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

public class Parser {
	
	private Map<Relation, Map<String, Field>> fieldMap;
	
	public QueryRelation parse(String query) {
		
		TGSqlParser parser = new TGSqlParser(EDbVendor.dbvansi);
		parser.setSqltext(query);
		
		if (parser.parse() != 0) {
			throw new IllegalArgumentException(parser.getErrormessage());
		}
		if (parser.getSqlstatements().size() > 1) {
			throw new UnsupportedOperationException("Only one statement per query is supported at this time.");
		}
		if (parser.getSqlstatements().get(0).sqlstatementtype != ESqlStatementType.sstselect) {
			throw new UnsupportedOperationException("Only SELECT queries are supported at this time.");
		}
		
		return this.parse((TSelectSqlStatement)parser.getSqlstatements().get(0));
	}
	
	private QueryRelation parse(TSelectSqlStatement statement) {
		if (statement.isCombinedQuery()) {
			throw new UnsupportedOperationException("Combined SELECT queries are not supported at this time.");
		}
		return this.parse(statement, null);
	}
	
	private QueryRelation parse(TSelectSqlStatement statement, List<Relation> parentRelations) {
		
		// FROM
		List<Relation> relations = new LinkedList<Relation>();
		for (int i = 0; i < statement.joins.size(); i++) {
			TJoin join = statement.joins.getJoin(i);
			switch (join.getKind()) {
			case TBaseType.join_source_fake:
				TTable table = join.getTable();
				Relation relation = null;
				if (table.subquery == null) {
					relation = new NamedRelation(join.getTable().getName());
				} else {
					relation = this.parse(table.subquery, parentRelations);
				}
				if (table.getAliasClause() != null) {
					relation.setAlias(table.getAliasClause().toString());
				}
				relations.add(relation);
				break;
			default:
				throw new UnsupportedOperationException("JOIN clauses in queries are not supported at this time, please specify your join conditions in the WHERE clause.");
			}
		}
		
		List<Relation> relationCandidates = new LinkedList<Relation>(relations);
		if (parentRelations != null) {
			relationCandidates.addAll(parentRelations);
		}
		
		// SELECT
		List<Field> fields = new LinkedList<Field>();
		for (int i = 0; i < statement.getResultColumnList().size(); i++) {
			fields.add(this.extractField(statement.getResultColumnList().getResultColumn(i), relationCandidates));
		}
		
		QueryRelation relation = new QueryRelation(fields, relations);
		
		// WHERE
		if (statement.getWhereClause() != null && statement.getWhereClause().getCondition() != null) {
			List<Qualifier> qualifiers = new LinkedList<Qualifier>();
			for (TExpression expression : this.extractExpressions(statement.getWhereClause().getCondition())) {
				Operator operator;
				Operand operand1 = null;
				Operand operand2 = null;
				TExpression leftOperand = expression.getLeftOperand();
				TExpression rightOperand = expression.getRightOperand();
				switch (expression.getExpressionType()) {
				case simple_comparison_t:
					if (leftOperand.getExpressionType() != EExpressionType.simple_object_name_t) {
						throw new UnsupportedOperationException(String.format(
								"Only field references are supported as left operands of comparison operators at this time: %s",
								leftOperand
								));
					}
					operator = Operator.forOperatorString(expression.getOperatorToken().toString());
					if (operator == null) {
						throw new UnsupportedOperationException(String.format(
								"Operator %s is not supported at this time.",
								expression.getOperatorToken()
								));
					}
					switch (rightOperand.getExpressionType()) {
					case simple_object_name_t:
						operand2 = this.extractField(rightOperand, relationCandidates);
						break;
					case simple_constant_t:
						operand2 = new LiteralOperand(rightOperand.toString());
						break;
					case subquery_t:
						operand2 = this.parse(rightOperand.getSubQuery(), relationCandidates);
						break;
					default:
						throw new UnsupportedOperationException(String.format(
								"Only field references, constants, and subqueries are supported as right operands of comparison operators at this time: %s",
								rightOperand
								));
					}
					qualifiers.add(new Qualifier(
							operator,
							Arrays.<Operand>asList(
									this.extractField(leftOperand, relationCandidates),
									operand2
									)
							));
					break;
				case logical_not_t:
					if (expression.getRightOperand().getExpressionType() == EExpressionType.exists_t) {
						qualifiers.add(new Qualifier(
								Operator.NOT_EXISTS,
								this.parse(expression.getRightOperand().getSubQuery(), relationCandidates)
								));
					} else {
						throw new IllegalArgumentException(String.format(
								"Expressions with operator NOT are only supported in combination with IN and EXISTS operators at this time: %s",
								expression.getRightOperand()
								));
					}
					break;
				case exists_t:
					qualifiers.add(new Qualifier(
							Operator.EXISTS,
							this.parse(expression.getSubQuery(), relationCandidates)
							));
					break;
				case in_t:
					if (rightOperand.getExpressionType() != EExpressionType.subquery_t) {
						throw new UnsupportedOperationException(String.format(
								"Only subqueries are supported as right operands of IN operators at this time: %s." +
								rightOperand
								));
					}
					qualifiers.add(new Qualifier(
							expression.getNotToken() == null ? Operator.IN : Operator.NOT_IN,
							Arrays.<Operand>asList(
									this.extractField(expression.getLeftOperand(), relationCandidates),
									this.parse(expression.getRightOperand().getSubQuery(), relationCandidates)
									)
							));
					break;
				case between_t:
					switch (leftOperand.getExpressionType()) {
					case simple_object_name_t:
						operand2 = this.extractField(leftOperand, relationCandidates);
						break;
					case simple_constant_t:
						operand2 = new LiteralOperand(leftOperand.toString());
						break;
					case subquery_t:
						operand2 = this.parse(leftOperand.getSubQuery(), relationCandidates);
						break;
					default:
						throw new UnsupportedOperationException(String.format(
								"Only field references, constants, and subqueries are supported as middle operands of BETWEEN operator at this time: %s",
								leftOperand
								));
					}
					Operand operand3 = null;
					switch (rightOperand.getExpressionType()) {
					case simple_object_name_t:
						operand3 = this.extractField(rightOperand, relationCandidates);
						break;
					case simple_constant_t:
						operand3 = new LiteralOperand(rightOperand.toString());
						break;
					case subquery_t:
						operand3 = this.parse(rightOperand.getSubQuery(), relationCandidates);
						break;
					default:
						throw new UnsupportedOperationException(String.format(
								"Only field references, constants, and subqueries are supported as right operands of BETWEEN operator at this time: %s",
								rightOperand
								));
					}
					TExpression betweenOperand = expression.getBetweenOperand();
					if (betweenOperand.getExpressionType() != EExpressionType.simple_object_name_t) {
						throw new UnsupportedOperationException(String.format(
								"Only field references are supported as left operands of BETWEEN operator at this time: %s",
								betweenOperand
								));
					}
					operand1 = this.extractField(betweenOperand, relationCandidates);
					qualifiers.add(new Qualifier(
							Operator.BETWEEN,
							Arrays.<Operand>asList(
									operand1,
									operand2,
									operand3
									)
							));
					break;
				default:
					throw new UnsupportedOperationException(String.format(
							"Expressions with operator %s are not supported at this time.",
							expression.getOperatorToken()
							));
				}
			}
			relation.setQualifiers(qualifiers);
		}
		
		// GROUP BY + HAVING
		if (statement.getGroupByClause() != null) {
			List<NamedField> grouping = new LinkedList<NamedField>();
			TGroupByItemList groupByItems = statement.getGroupByClause().getItems();
			for (int i = 0; i < groupByItems.size(); i++) {
				TGroupByItem groupByItem = groupByItems.getGroupByItem(i);
				if (groupByItem.getRollupCube() != null || groupByItem.getGroupingSet() != null) {
					throw new UnsupportedOperationException(String.format(
							"The following GROUP BY item is unsupported at this time: %s.",
							groupByItem
							));
				}
				grouping.add((NamedField)this.extractField(groupByItem.getExpr(), relations)); // GROUP BY cannot be on fields from outer queries, hence not relationCandidates
			}
			relation.setGrouping(grouping);
			
			// HAVING
			if (statement.getGroupByClause().getHavingClause() != null) {
				throw new UnsupportedOperationException("HAVING clause is unsupported at this time.");
			}
		}
		
		return relation;
	}
	
	private Field extractField(TResultColumn column, List<Relation> relationCandidates) {
		Field field = this.extractField(column.getExpr(), relationCandidates);
		if (column.getAliasClause() != null) {
			field.setAlias(column.getAliasClause().toString());
		}
		return field;
	}
	
	private Field extractField(TExpression expression, List<Relation> relationCandidates) {
		
		if (expression.getExpressionType() != EExpressionType.simple_object_name_t && expression.getExpressionType() != EExpressionType.function_t) {
			throw new UnsupportedOperationException(String.format(
					"Expression SELECT fields such as %s are not supported at this time.",
					expression
					));
		}
		Aggregate aggregate = null;
		if (expression.getExpressionType() == EExpressionType.function_t) {
			TFunctionCall function = expression.getFunctionCall();
			TExpressionList aggregateArguments = function.getArgs();
			if (aggregateArguments.size() != 1) {
				throw new UnsupportedOperationException(String.format(
						"Only 1-argument aggregate functions are supported at this time: %s.",
						expression
						));
			}
			aggregate = Aggregate.forFunctionName(function.getFunctionName().toString());
			if (aggregate == null) {
				throw new UnsupportedOperationException(String.format(
						"Aggregate function %s is not supported at this time.",
						function
						));
			}
			expression = aggregateArguments.getExpression(0);
		}
		if (aggregate != null && expression.getExpressionType() != EExpressionType.simple_object_name_t) {
			throw new UnsupportedOperationException(String.format(
					"Only aggregates on simple columns are supported at this time, cannot do %s on %s.",
					aggregate,
					expression
					));
					
		}
		String[] fieldParts = expression.toString().split("\\.");
		if (fieldParts.length != 2) {
			throw new IllegalArgumentException(String.format(
					"Relation of field %s is unspecified, please specify field names as {relation name or alias}.{field name}.",
					expression
					));
		}
		String tableName = fieldParts[0];
		String fieldName = fieldParts[1];
		Relation relation = null;
		for (ListIterator<Relation> it = relationCandidates.listIterator(relationCandidates.size()); it.hasPrevious(); ) {
			Relation candidateRelation = it.previous();
			if ((candidateRelation.getAlias() != null && candidateRelation.getAlias().equals(tableName)) || (candidateRelation.getAlias() == null && (candidateRelation instanceof NamedRelation) && ((NamedRelation)candidateRelation).getName().equals(tableName))) {
				relation = candidateRelation;
				break;
			}
		}
		if (relation == null) {
			throw new IllegalArgumentException(String.format(
					"Could not find relation %s for field %s.%s.",
					tableName,
					tableName,
					fieldName
					));
		}
		if (this.fieldMap == null) {
			this.fieldMap = new HashMap<Relation, Map<String, Field>>();
		}
		if (this.fieldMap.get(relation) == null) {
			this.fieldMap.put(relation, new HashMap<String, Field>());
		}
		if (this.fieldMap.get(relation).get(fieldName) == null) {
			this.fieldMap.get(relation).put(fieldName, new NamedField(relation, fieldName));
		}
		Field field = this.fieldMap.get(relation).get(fieldName);
		if (aggregate != null) {
			String aggregatedFieldName = aggregate + "(" + fieldName + ")";
			if (this.fieldMap.get(relation).get(aggregatedFieldName) == null) {
				this.fieldMap.get(relation).put(aggregatedFieldName, new AggregateField(aggregate, (NamedField)field));
			}
			field = this.fieldMap.get(relation).get(aggregatedFieldName);
		}
		
		return field;
	}
	
	private List<TExpression> extractExpressions(TExpression expression) {
		List<TExpression> expressions = new LinkedList<TExpression>();
		if (expression.getExpressionType() != EExpressionType.logical_and_t) {
			expressions.add(expression);
		} else {
			expressions.addAll(this.extractExpressions(expression.getLeftOperand()));
			expressions.addAll(this.extractExpressions(expression.getRightOperand()));
		}
		return expressions;
	}
	
	public static void main(String[] args) {
		System.out.println(new Parser().parse("select sum(test.a) from test where test.k in (select blah.b from blah where blah.c = test.d) and test.e = 100"));
		System.out.println(new Parser().parse("SELECT S.id FROM S   WHERE NOT EXISTS ( SELECT C.id FROM C WHERE NOT EXISTS (        SELECT T.sid                FROM T          WHERE S.id = T.sid AND              C.id = T.cid                        )              )"));
		System.out.println(new Parser().parse("SELECT myS.id FROM (SELECT S.id FROM S ) myS,(SELECT T.sid FROM T) myT WHERE myS.id = myT.sid"));
		System.out.println(new Parser().parse("SELECT shipping.supp_nation, shipping.cust_nation, shipping.l_year, SUM(shipping.volume) AS revenue FROM (SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation, lineitem.l_shipdate AS l_year, lineitem.l_extendedprice AS volume FROM supplier, lineitem, orders, customer, nation n1, nation n2 WHERE supplier.s_suppkey = lineitem.l_suppkey AND orders.o_orderkey = lineitem.l_orderkey AND customer.c_custkey = orders.o_custkey AND supplier.s_nationkey = n1.n_nationkey AND customer.c_nationkey = n2.n_nationkey AND n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE' AND lineitem.l_shipdate BETWEEN '1995-01-01' AND '1996-12-31') shipping GROUP BY shipping.supp_nation, shipping.cust_nation, shipping.l_year"));
	}
}
