package ch.epfl.ad.app;

import java.sql.SQLException;
import java.util.Arrays;

import ch.epfl.ad.AbstractQuery;
import ch.epfl.ad.db.DatabaseManager;
import ch.epfl.ad.db.parsing.Aggregate;
import ch.epfl.ad.db.parsing.AggregateField;
import ch.epfl.ad.db.parsing.ExpressionOperand;
import ch.epfl.ad.db.parsing.Field;
import ch.epfl.ad.db.parsing.NamedField;
import ch.epfl.ad.db.parsing.NamedRelation;
import ch.epfl.ad.db.parsing.Operand;
import ch.epfl.ad.db.parsing.Operator;
import ch.epfl.ad.db.parsing.Qualifier;
import ch.epfl.ad.db.parsing.QueryRelation;
import ch.epfl.ad.db.parsing.Relation;
import ch.epfl.ad.db.querytackling.QueryGraph;

public class TestQuery extends AbstractQuery {

	@Override
	public void run(String[] args) throws SQLException, InterruptedException {
		
		/*if (args.length < 1) {
		    System.out.println("Arguments: config-file");
		    System.exit(1);
		}
		
		DatabaseManager dbManager = createDatabaseManager(args[0]);
		
		dbManager.setResultShipmentBatchSize(5000);*/
		
		Relation s = new NamedRelation("S");
		Relation c = new NamedRelation("C");
		Relation t = new NamedRelation("T");
		
		NamedField sId = new NamedField(s, "id");
		NamedField cId = new NamedField(c, "id");
		NamedField tSid = new NamedField(t, "sid");
		NamedField tCid = new NamedField(t, "cid");
		
		/* SELECT S.id
		 * FROM S
		 * WHERE NOT EXISTS (
		 *                   SELECT C.id
		 *                   FROM C
		 *                   WHERE NOT EXISTS (
		 *                                     SELECT T.sid
		 *                                     FROM T
		 *                                     WHERE S.id = T.sid AND
		 *                                           C.id = T.cid
		 *                                    )
		 *                  )
		 */
		QueryRelation query = new QueryRelation(
				sId,
				s
				).setQualifier(new Qualifier(Operator.NOT_EXISTS, new QueryRelation(
						cId,
						c
						).setQualifier(new Qualifier(Operator.NOT_EXISTS, new QueryRelation(
								tSid,
								t
								).setQualifiers(Arrays.asList(
										new Qualifier(Operator.EQUALS, Arrays.<Operand>asList(sId, tSid)),
										new Qualifier(Operator.EQUALS, Arrays.<Operand>asList(cId, tCid))
										)
								))
						))
				);
		
		System.out.println(query);
		
		QueryGraph graph = new QueryGraph(query);
		System.out.println(graph);
		
		// (SELECT S.id FROM S) myS
		Relation sId_S = new QueryRelation(sId, s).setAlias("myS");
		
		// (SELECT T.sid FROM T) myT
		Relation tSid_T = new QueryRelation(tSid, t).setAlias("myT");
		
		NamedField mySId = new NamedField(sId_S, "id");
		NamedField myTSid = new NamedField(tSid_T, "sid");
		
		/* SELECT myS.id
		 * FROM (
		 *       SELECT S.id
		 *       FROM S
		 *      ) myS,
		 *      (
		 *       SELECT T.sid
		 *       FROM T
		 *      ) myT
		 * WHERE myS.id = myT.sid
		 */
		QueryRelation query2 = new QueryRelation(
				mySId,
				Arrays.<Relation>asList(sId_S, tSid_T)
				).setQualifier(
						new Qualifier(Operator.EQUALS, Arrays.<Operand>asList(mySId, myTSid))
				);
		
		System.out.println(query2);
		
		QueryGraph graph2 = new QueryGraph(query2);
		System.out.println(graph2);
		
		Relation p = new NamedRelation("P");
		NamedField pCid = new NamedField(p, "cid");
		
		// (SELECT T.sid, P.cid FROM P, T WHERE P.cid = C.id) myPT
		Relation tSidpCid_PT = new QueryRelation(
				Arrays.<Field>asList(tSid, pCid),
				Arrays.<Relation>asList(p, t)
				).setQualifier(
						new Qualifier(Operator.EQUALS, Arrays.<Operand>asList(pCid, cId))
				).setAlias("myPT");
		
		NamedField myPTSid = new NamedField(tSidpCid_PT, "sid");
		
		// SELECT S.id FROM S WHERE s.id = myPT.sid
		Relation sId_S2 = new QueryRelation(
				sId,
				s
				).setQualifier(
						new Qualifier(Operator.EQUALS, Arrays.<Operand>asList(sId, myPTSid))
				);
		
		/* SELECT C.id
		 * FROM C
		 * WHERE EXISTS (
		 *               SELECT myPT.sid
		 *               FROM (
		 *                     SELECT T.sid, P.cid
		 *                     FROM P, T
		 *                     WHERE P.cid = C.id
		 *                    ) myPT
		 *               WHERE EXISTS (
		 *                             SELECT S.id
		 *                             FROM S
		 *                             WHERE S.id = myPT.sid
		 *                            )
		 *              )
		 */
		QueryRelation query3 = new QueryRelation(
				cId,
				c
				).setQualifier(
						new Qualifier(Operator.EXISTS, new QueryRelation(
								myPTSid,
								tSidpCid_PT
								).setQualifier(
										new Qualifier(Operator.EXISTS, sId_S2)
								))
				);
		
		System.out.println(query3);
		
		QueryGraph graph3 = new QueryGraph(query3);
		System.out.println(graph3);
		
		/* TPCH Query 7 */
		
		Relation supplier = new NamedRelation("supplier");
		Relation lineitem = new NamedRelation("lineitem");
		Relation orders = new NamedRelation("orders");
		Relation customer = new NamedRelation("customer");
		Relation nation1 = new NamedRelation("nation").setAlias("n1");
		Relation nation2 = new NamedRelation("nation").setAlias("n2");

		NamedField s_suppkey = new NamedField(supplier, "s_suppkey");
		NamedField l_suppkey = new NamedField(lineitem, "l_suppkey");
		NamedField o_orderkey = new NamedField(orders, "o_orderkey");
		NamedField l_orderkey = new NamedField(lineitem, "l_orderkey");
		NamedField c_custkey = new NamedField(customer, "c_custkey");
		NamedField o_custkey = new NamedField(orders, "o_custkey");
		NamedField s_nationkey = new NamedField(supplier, "s_nationkey");
		NamedField n1_nationkey = new NamedField(nation1, "n_nationkey");
		NamedField c_nationkey = new NamedField(customer, "c_nationkey");
		NamedField n2_nationkey = new NamedField(nation2, "n_nationkey");
		NamedField n1_name = new NamedField(nation1, "n_name").setAlias("supp_nation");
		NamedField n2_name = new NamedField(nation2, "n_name").setAlias("cust_nation");
		NamedField l_shipdate = new NamedField(lineitem, "l_shipdate").setAlias("l_year");
		NamedField l_extendedprice = new NamedField(lineitem, "l_extendedprice").setAlias("volume");
		//NamedField l_discount = new NamedField(lineitem, "l_discount");
		
		Relation q7Shipping = new QueryRelation(
				Arrays.<Field>asList(
						n1_name,
						n2_name,
						l_shipdate,
						l_extendedprice
						),
					Arrays.<Relation>asList(
						supplier,
						lineitem,
						orders,
						customer,
						nation1,
						nation2
						)
					).setQualifiers(Arrays.asList(
						new Qualifier(
							Operator.EQUALS,
							Arrays.<Operand>asList(
								s_suppkey,
								l_suppkey
								)
							),
						new Qualifier(
							Operator.EQUALS,
							Arrays.<Operand>asList(
								o_orderkey,
								l_orderkey
								)
							),
						new Qualifier(
							Operator.EQUALS,
							Arrays.<Operand>asList(
								c_custkey,
								o_custkey
								)
							),
						new Qualifier(
							Operator.EQUALS,
							Arrays.<Operand>asList(
								s_nationkey,
								n1_nationkey
								)
							),
						new Qualifier(
							Operator.EQUALS,
							Arrays.<Operand>asList(
								c_nationkey,
								n2_nationkey
								)
							),
						new Qualifier(
							Operator.EQUALS,
							Arrays.<Operand>asList(
								n1_name,
								new ExpressionOperand("\"GERMANY\"")
								)
							),
						new Qualifier(
							Operator.EQUALS,
							Arrays.<Operand>asList(
								n2_name,
								new ExpressionOperand("\"FRANCE\"")
								)
							),
						new Qualifier(
							Operator.BETWEEN,
							Arrays.<Operand>asList(
								l_shipdate,
								new ExpressionOperand("1995-01-01"),
								new ExpressionOperand("1996-12-31")
									)
								)
							)
					).setAlias("shipping");
		
		NamedField supp_nation = new NamedField(q7Shipping, n1_name);
		NamedField cust_nation = new NamedField(q7Shipping, n2_name);
		NamedField l_year = new NamedField(q7Shipping, l_shipdate);
		AggregateField revenue = new AggregateField(
				Aggregate.SUM,
				new NamedField(q7Shipping, l_extendedprice)
				).setAlias("revenue");
		
		QueryRelation q7 = new QueryRelation(
				Arrays.<Field>asList(
						supp_nation,
						cust_nation,
						l_year,
						revenue
						),
				q7Shipping
				).setGrouping(Arrays.asList(
						supp_nation,
						cust_nation,
						l_year
						)
				
				).setGroupingQualifier(new Qualifier(Operator.GREATER_THAN, Arrays.asList(revenue, new ExpressionOperand("5"))));
		
		System.out.println(q7);
		
		QueryGraph graphQ7 = new QueryGraph(q7);
		System.out.println(graphQ7);
	}
	
	public static void main(String[] args) throws SQLException, InterruptedException {
		new TestQuery().run(args);
	}
}
