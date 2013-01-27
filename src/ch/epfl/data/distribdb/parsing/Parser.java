package ch.epfl.data.distribdb.parsing;

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
import gudusoft.gsqlparser.nodes.TLimitClause;
import gudusoft.gsqlparser.nodes.TOffsetClause;
import gudusoft.gsqlparser.nodes.TOrderByItem;
import gudusoft.gsqlparser.nodes.TOrderByItemList;
import gudusoft.gsqlparser.nodes.TResultColumn;
import gudusoft.gsqlparser.nodes.TSelectSqlNode;
import gudusoft.gsqlparser.nodes.TTable;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

/**
 * Parser of SQL queries.
 * 
 * @author Artyom Stetsenko
 */
public class Parser {
	
	/**
	 * Map of Field.toString() -> Field per subquery, used for reusing existing query fields
	 * when they appear in several places.
	 */
	private Map<TSelectSqlStatement, Map<String, Field>> statementFieldMap;
	
	/**
	 * Map of Field.getAlias() -> Field per subquery, used for mapping field references
	 * unqualified by relations to fields (used in GROUP BY, HAVING, and ORDER BY clauses).
	 */
	private Map<TSelectSqlStatement, Map<String, Field>> statementAliasedFieldMap;
	
	/**
	 * Parses a query and returns its parse tree.
	 * 
	 * @param query
	 *                query string to parse
	 * @return query parse tree
	 */
	public static QueryRelation parse(String query) {
		
		TGSqlParser parser = new TGSqlParser(EDbVendor.dbvpostgresql);
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
		
		return new Parser().parse((TSelectSqlStatement)parser.getSqlstatements().get(0));
	}
	
	/**
	 * Parses the given SQL statement.
	 * 
	 * @param statement
	 *                the statement to parse
	 * @return the resulting parse tree
	 */
	private QueryRelation parse(TSelectSqlStatement statement) {
		return this.parse(statement, null);
	}
	
	/**
	 * Parses the given SQL statement.
	 * 
	 * @param statement
	 *                the statement to parse
	 * @param parentRelations
	 *                if this statement is a subquery: relations of the parent query 
	 * @return the resulting parse tree
	 */
	private QueryRelation parse(TSelectSqlStatement statement, List<Relation> parentRelations) {
		
		if (statement.isCombinedQuery()) {
			throw new UnsupportedOperationException("Combined SELECT queries are not supported at this time.");
		}
		
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
		/* no correlated queries
		if (parentRelations != null) {
			relationCandidates.addAll(parentRelations);
		}
		*/
		
		// SELECT
		List<Field> fields = new LinkedList<Field>();
		for (int i = 0; i < statement.getResultColumnList().size(); i++) {
			fields.add(this.extractField(
					statement,
					statement.getResultColumnList().getResultColumn(i),
					relations
					)); // not candidateRelations because no support for outer query fields in SELECT
		}
		
		QueryRelation relation = new QueryRelation(fields, relations);
		
		// DISTINCT
		if (statement.getSelectDistinct() != null && statement.getSelectDistinct().getDistinctType() == 1) {
			relation.setDistinctFields();
		}
		
		// WHERE
		if (statement.getWhereClause() != null && statement.getWhereClause().getCondition() != null) {
			List<Qualifier> qualifiers = new LinkedList<Qualifier>();
			for (TExpression expression : this.extractExpressions(statement.getWhereClause().getCondition())) {
				qualifiers.add(this.extractQualifier(statement, expression, relationCandidates));
			}
			relation.setQualifiers(qualifiers);
		}
		
		// GROUP BY + HAVING
		if (statement.getGroupByClause() != null) {
			TGroupByItemList groupByItems = statement.getGroupByClause().getItems();
			if (groupByItems != null) {
				List<Field> grouping = new LinkedList<Field>();
				for (int i = 0; i < groupByItems.size(); i++) {
					TGroupByItem groupByItem = groupByItems.getGroupByItem(i);
					if (groupByItem.getRollupCube() != null || groupByItem.getGroupingSet() != null) {
						throw new UnsupportedOperationException(String.format(
								"The following GROUP BY item is unsupported at this time: %s.",
								groupByItem
								));
					}
					Field field = this.extractField(
							statement,
							groupByItem.getExpr(),
							relations, // GROUP BY cannot be on fields from outer queries, hence not relationCandidates
							true
							);
					if (field instanceof AggregateField) {
						throw new IllegalArgumentException("Aggregate fields cannot appear in GROUP BY list.");
					}
					grouping.add(field);
				}
				relation.setGrouping(grouping);
			}
			
			// HAVING
			if (statement.getGroupByClause().getHavingClause() != null) {
				List<Qualifier> groupingQualifiers = new LinkedList<Qualifier>();
				for (TExpression expression : this.extractExpressions(statement.getGroupByClause().getHavingClause())) {
					groupingQualifiers.add(this.extractQualifier(statement, expression, relationCandidates));
				}
				relation.setGroupingQualifiers(groupingQualifiers);
			}
		}
		
		// ORDER BY
		if (statement.getOrderbyClause() != null) {
			List<OrderingItem> ordering = new LinkedList<OrderingItem>();
			TOrderByItemList orderByItems = statement.getOrderbyClause().getItems();
			for (int i = 0; i < orderByItems.size(); i++) {
				TOrderByItem orderByItem = orderByItems.getOrderByItem(i);
				String orderingTypeName = null;
				switch (orderByItem.getSortType()) {
				case 0:
					// err... with parser initialized with PostgresSQL vendor, getSortType() always returns 0...
					String[] tokens = orderByItem.toString().trim().split(" ");
					if (tokens[tokens.length - 1].equalsIgnoreCase("asc")) {
						orderingTypeName = "ASC";
					} else if (tokens[tokens.length - 1].equalsIgnoreCase("desc")) {
						orderingTypeName = "DESC";
					}
					break;
				case 1:
					orderingTypeName = "ASC";
					break;
				case 2:
					orderingTypeName = "DESC";
					break;
				default: // should never be the case
					throw new UnsupportedOperationException(String.format(
							"Ordering type %s is not supported: %s.",
							orderByItem.getSortType(),
							orderByItem
							));
				}
				Field field = this.extractField(statement, orderByItem.getSortKey(), relations, true);
				if (relation.areFieldsDistinct() && !fields.contains(field)) {
					throw new IllegalArgumentException("For SELECT DISTINCT queries, fields from ORDER BY clause must appear in the SELECT list.");
				}
				ordering.add(new OrderingItem(
						field,
						OrderingType.forTypeName(orderingTypeName)
						));
			}
			relation.setOrdering(ordering);
		}
		
		// LIMIT y OFFSET x / OFFSET x FETCH NEXT y ROWS ONLY
		if (((TSelectSqlNode)statement.rootNode).getSelectLimit() != null) {
			
			// LIMIT y / FETCH NEXT y rows ONLY
			TLimitClause limitClause = ((TSelectSqlNode)statement.rootNode).getSelectLimit().getLimitClause();
			if (limitClause != null) {
				if (limitClause.getSelectFetchFirstValue() != null) {
					relation.setNumRows(Integer.parseInt(limitClause.getSelectFetchFirstValue().toString()));
				} else if (limitClause.getSelectLimitValue() != null) {
					relation.setNumRows(Integer.parseInt(limitClause.getSelectLimitValue().toString()));
				}
			}
			
			// OFFSET x
			TOffsetClause offsetClause = ((TSelectSqlNode)statement.rootNode).getSelectLimit().getOffsetClause();
			if (offsetClause != null) {
				String[] tokens = offsetClause.toString().trim().split(" ");
				relation.setOffset(Integer.parseInt(tokens[tokens.length - 1]));
			}
		}
		
		return relation;
	}
	
	/**
	 * Extracts the Field from the given SELECT column.
	 * 
	 * @param statement
	 *                SQL statement to which the expression belongs
	 * @param column
	 *                the column to extract the Field from
	 * @param relationCandidates
	 *                relations to which the Field may belong
	 * @return the extracted Field
	 */
	private Field extractField(TSelectSqlStatement statement, TResultColumn column, List<Relation> relationCandidates) {
		Field field = this.extractField(statement, column.getExpr(), relationCandidates, false);
		if (column.getAliasClause() != null) {
			String alias = column.getAliasClause().toString();
			field.setAlias(alias);
			if (this.statementAliasedFieldMap == null) {
				this.statementAliasedFieldMap = new HashMap<TSelectSqlStatement, Map<String, Field>>();
			}
			if (this.statementAliasedFieldMap.get(statement) == null) {
				this.statementAliasedFieldMap.put(statement, new HashMap<String, Field>());
			}
			if (this.statementAliasedFieldMap.get(statement).get(alias) == null) {
				this.statementAliasedFieldMap.get(statement).put(alias, field);
			}
			field = this.statementAliasedFieldMap.get(statement).get(alias);
		}
		return field;
	}
	
	/**
	 * Extracts the Field from the given expression.
	 * 
	 * @param statement
	 *                SQL statement to which the expression belongs
	 * @param expression
	 *                the expression to extract the Field from
	 * @param relationCandidates
	 *                relations to which the Field may belong
	 * @param aliasesAllowed
	 *                flag indicating that the Field may be an alias of one of the fields in the current
	 *                query's SELECT list (and thus will not be qualified by a relation)
	 * @return the extracted Field
	 */
	private Field extractField(TSelectSqlStatement statement, TExpression expression, List<Relation> relationCandidates, boolean aliasesAllowed) {
		
		Field field = null;
		Aggregate aggregate = null;
		
		switch (expression.getExpressionType()) {
		
		case simple_constant_t: // e.g. 1
			field = new LiteralField(expression.getConstantOperand().toString());
			break;
			
		case arithmetic_plus_t:
		case arithmetic_minus_t:
		case arithmetic_times_t:
		case arithmetic_divide_t:
		case arithmetic_modulo_t:
		case exponentiate_t:
		case parenthesis_t: // e.g. a * b + (c % d)
			Field field1 = this.extractField(statement, expression.getLeftOperand(), relationCandidates, aliasesAllowed);
			Field field2 = null;
			if (expression.getRightOperand() != null) {
				field2 = this.extractField(statement, expression.getRightOperand(), relationCandidates, aliasesAllowed);
			}
			StringBuilder fieldExpression = new StringBuilder();
			List<Field> fieldFields = new LinkedList<Field>();
			int count1 = 1;
			if (field1 instanceof ExpressionField) {
				ExpressionField expressionField1 = (ExpressionField)field1;
				count1 = expressionField1.getFields().size();
				fieldExpression.append(expressionField1.getExpression());
				fieldFields.addAll(expressionField1.getFields());
			} else if (field1 instanceof LiteralField) {
				fieldExpression.append(field1);
			} else {
				fieldExpression.append(ExpressionField.PLACEHOLDER + "1");
				fieldFields.add(field1);
			}
			if (expression.getExpressionType() == EExpressionType.parenthesis_t) {
				fieldExpression.insert(0, expression.getStartToken()); // "("
			} else {
				fieldExpression.append(" ").append(expression.getOperatorToken()).append(" ");
			}
			if (field2 != null && field2 instanceof ExpressionField) {
				ExpressionField expressionField2 = (ExpressionField)field2;
				String field2Expression = expressionField2.getExpression();
				for (int i = 1; i <= expressionField2.getFields().size(); i++) {
					field2Expression = field2Expression.replaceAll(ExpressionField.PLACEHOLDER + i, ExpressionField.PLACEHOLDER + (i + count1));
				}
				fieldExpression.append(field2Expression);
				for (Field field2Field : expressionField2.getFields()) {
					if (!fieldFields.contains(field2Field)) {
						fieldFields.add(field2Field);
					}
				}
			} else if (field2 != null && field2 instanceof LiteralField) {
				fieldExpression.append(field2);
			} else if (field2 != null) {
				fieldExpression.append(ExpressionField.PLACEHOLDER + (fieldFields.size() + 1));
				fieldFields.add(field2);
			}
			if (expression.getExpressionType() == EExpressionType.parenthesis_t) {
				fieldExpression.append(expression.getEndToken()); // ")"
			}
			field = new ExpressionField(fieldExpression.toString(), fieldFields);
			break;
			
		case function_t: // e.g. func(a), can be aggregate or not
			TFunctionCall function = expression.getFunctionCall();
			String functionName = function.getFunctionName().toString();
			aggregate = Aggregate.forFunctionName(functionName);
			TExpressionList functionArguments = function.getArgs();
			if (aggregate != null) { // known aggregate function
				if (functionArguments.size() != 1) {
					throw new UnsupportedOperationException(String.format(
							"Only 1-argument aggregate functions are supported at this time: %s.",
							expression
							));
				} else if (functionArguments.getExpression(0).toString().equals("*")) {
					field = new AggregateField(aggregate, new LiteralField("*"));
				} else {
					field = new AggregateField(aggregate, this.extractField(
							statement,
							functionArguments.getExpression(0),
							relationCandidates,
							aliasesAllowed
							));
				}
				
				// hack to identify whether this aggregate calls for distinct values (parser does not seem to
				// provide an API for this), will not work in some obscure circumstances
				if (expression.toString().substring(expression.toString().indexOf('(') + 1).trim().split(" ")[0].equalsIgnoreCase("distinct")) {
					((AggregateField)field).setDistinct();
				}
			} else { // assuming function call (non-aggregate)
				switch (function.getFunctionType()) {
				case extract_t:
					field = new FunctionField(
							functionName,
							new ExpressionField(
									function.getExtract_time_token() + " FROM " + ExpressionField.PLACEHOLDER + "1",
									this.extractField(statement, function.getExpr1(), relationCandidates, aliasesAllowed)
									)
							);
					break;
				default:
					throw new UnsupportedOperationException(String.format(
							"Function %s is not supported at this time.",
							function.toString().toUpperCase()
							));
				}
			}
			break;
			
		case simple_object_name_t: // e.g. relation.f
			String[] fieldParts = expression.toString().split("\\.");
			if (fieldParts.length != 2) { // try to match with aliased fields first
				if (fieldParts.length == 1 && aliasesAllowed && this.statementAliasedFieldMap != null && this.statementAliasedFieldMap.get(statement) != null && this.statementAliasedFieldMap.get(statement).get(fieldParts[0]) != null) {
					field = this.statementAliasedFieldMap.get(statement).get(fieldParts[0]);
					break;
				} else {
					throw new IllegalArgumentException(String.format(
							"Relation of field %s is unspecified, please specify field names as {relation name or alias}.{field name}.",
							expression
							));
				}
			}
			String tableName = fieldParts[0];
			String fieldName = fieldParts[1];
			if (fieldName.equals("*")) {
				throw new IllegalArgumentException("* fields are not supported at this time.");
			}
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
						"Could not find relation %s for field %s.%s. Note that correlated queries are not supported at this time.",
						tableName,
						tableName,
						fieldName
						));
			}
			field = new NamedField(relation, fieldName);
			break;
			
		default:
			throw new UnsupportedOperationException(String.format(
					"SELECT fields such as %s are not supported at this time.",
					expression
					));
		}
		
		String fieldString = field.toString();
		if (this.statementFieldMap == null) {
			this.statementFieldMap = new HashMap<TSelectSqlStatement, Map<String, Field>>();
		}
		if (this.statementFieldMap.get(statement) == null) {
			this.statementFieldMap.put(statement, new HashMap<String, Field>());
		}
		if (this.statementFieldMap.get(statement).get(fieldString) == null) {
			this.statementFieldMap.get(statement).put(fieldString, field);
		}
		
		return this.statementFieldMap.get(statement).get(fieldString);
	}
	
	private Qualifier extractQualifier(TSelectSqlStatement statement, TExpression expression, List<Relation> relationCandidates) {
		Operator operator;
		TExpression leftOperand = expression.getLeftOperand();
		TExpression rightOperand = expression.getRightOperand();
		switch (expression.getExpressionType()) {
		case simple_comparison_t:
		case pattern_matching_t:
			operator = Operator.forOperatorString(
					(expression.getNotToken() != null ? "NOT " : "") +
						expression.getOperatorToken().toString().toUpperCase()
					);
			if (operator == null || expression.getQuantifier() != null || expression.getLikeEscapeOperand() != null) {
				throw new UnsupportedOperationException(String.format(
						"Expressions of type %s are not supported at this time.",
						expression
						));
			}
			return new Qualifier(
					operator,
					new LinkedList<Operand>(Arrays.<Operand>asList(
							this.extractOperand(statement, leftOperand, relationCandidates, false),
							this.extractOperand(statement, rightOperand, relationCandidates, false)
							)
					));
		case logical_not_t:
			if (rightOperand.getExpressionType() == EExpressionType.exists_t) {
				return new Qualifier(
						Operator.NOT_EXISTS,
						this.parse(rightOperand.getSubQuery(), relationCandidates)
						);
			} else {
				throw new IllegalArgumentException(String.format(
						"Expressions with operator NOT are only supported in combination with IN and EXISTS operators at this time: %s",
						rightOperand
						));
			}
		case exists_t:
			return new Qualifier(
					Operator.EXISTS,
					this.parse(expression.getSubQuery(), relationCandidates)
					);
		case in_t:
			if (rightOperand.getExpressionType() != EExpressionType.subquery_t) {
				throw new UnsupportedOperationException(String.format(
						"Only subqueries are supported as right operands of IN operators at this time: %s.",
						rightOperand
						));
			}
			return new Qualifier(
					expression.getNotToken() == null ? Operator.IN : Operator.NOT_IN,
					new LinkedList<Operand>(Arrays.<Operand>asList(
							this.extractField(statement, leftOperand, relationCandidates, false),
							this.parse(rightOperand.getSubQuery(), relationCandidates)
							)
					));
		case between_t:
			return new Qualifier(
					Operator.BETWEEN,
					new LinkedList<Operand>(Arrays.<Operand>asList(
							this.extractField(statement, expression.getBetweenOperand(), relationCandidates, false),
							this.extractOperand(statement, leftOperand, relationCandidates, false),
							this.extractOperand(statement, rightOperand, relationCandidates, false)
							)
					));
		default:
			throw new UnsupportedOperationException(String.format(
					"Qualifiers of type %s are not supported at this time.",
					expression.toString()
					));
		}
	}
	
	/**
	 * Extracts the Operand from the given expression.
	 * 
	 * @param statement
	 *                SQL statement to which the expression belongs
	 * @param expression
	 *                the expression to extract the Operand from
	 * @param relationCandidates
	 *                only if Operand is a Field: relations to which the Field may belong
	 * @param aliasesAllowed
	 *                only if Operand is a Field: flag indicating that the Field may be an alias
	 *                of one of the fields in the current query's SELECT list (and thus will not
	 *                be qualified by a relation)
	 * @return the extracted Operand
	 */
	private Operand extractOperand(TSelectSqlStatement statement, TExpression expression, List<Relation> relationCandidates, boolean aliasesAllowed) {
		switch (expression.getExpressionType()) {
		case simple_constant_t:
		case arithmetic_plus_t:
		case arithmetic_minus_t:
		case arithmetic_times_t:
		case arithmetic_divide_t:
		case arithmetic_modulo_t:
		case exponentiate_t:
		case parenthesis_t:
		case function_t:
		case simple_object_name_t:
			return this.extractField(statement, expression, relationCandidates, false);
		case subquery_t:
			return this.parse(expression.getSubQuery(), relationCandidates);
		default:
			throw new UnsupportedOperationException(String.format(
					"Operands of type %s are not supported at this time.",
					expression
					));
		}
	}
	
	/**
	 * Expands the given expression into a list of expressions connected by conjunction.
	 * 
	 * @param expression
	 *                expression to expand
	 * @return the resulting list of expressions
	 */
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
	
	public static void main(String[] args) { // tests
		//System.out.println(Parser.parse("select sum(test.a) from test where test.k in (select blah.b from blah where blah.c = test.d) and test.e = 100"));
		//System.out.println(Parser.parse("SELECT S.id FROM S   WHERE NOT EXISTS ( SELECT C.id FROM C WHERE NOT EXISTS (        SELECT T.sid                FROM T          WHERE S.id = T.sid AND              C.id = T.cid                        )              )"));
		System.out.println(Parser.parse("select nation.n_name from nation where crap"));
		System.out.println(Parser.parse("select nation.n_nationkey from nation having exists ( select region.r_regionkey from region)"));
		System.out.println(Parser.parse("select lineitem.l_orderkey, sum(lineitem.l_extendedprice * (1 - lineitem.l_discount)) as revenue, orders.o_orderdate, orders.o_shippriority from customer, orders, lineitem where customer.c_mktsegment = 'BUILDING' and customer.c_custkey = orders.o_custkey and lineitem.l_orderkey = orders.o_orderkey and orders.o_orderdate < '1995-03-15' and lineitem.l_shipdate > '1995-03-15' group by lineitem.l_orderkey, orders.o_orderdate, orders.o_shippriority order by revenue desc, orders.o_orderdate"));
		System.out.println(Parser.parse("select distinct count ( \n\t distinct r.a + 3) from r order by count(distinct r.a + 3)"));
		System.out.println(Parser.parse("select extract(year from r.a) from r where r.k * 3 - 5 > 4 offset 4/*sd*/ fetch next 3  rows only"));
		System.out.println(Parser.parse("SELECT myS.id FROM (SELECT S.id FROM S ) myS,(SELECT T.sid FROM T) myT WHERE myS.id = myT.sid"));
		System.out.println(Parser.parse("SELECT shipping.supp_nation, shipping.cust_nation, shipping.l_year, SUM(shipping.volume) AS revenue FROM (SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation, lineitem.l_shipdate AS l_year, lineitem.l_extendedprice AS volume FROM supplier, lineitem, orders, customer, nation n1, nation n2 WHERE supplier.s_suppkey = lineitem.l_suppkey AND orders.o_orderkey = lineitem.l_orderkey AND customer.c_custkey = orders.o_custkey AND supplier.s_nationkey = n1.n_nationkey AND customer.c_nationkey = n2.n_nationkey AND n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE' AND lineitem.l_shipdate BETWEEN '1995-01-01' AND '1996-12-31') shipping GROUP BY shipping.supp_nation, shipping.cust_nation, shipping.l_year"));
	}
}
