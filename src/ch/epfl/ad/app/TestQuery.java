package ch.epfl.ad.app;

import java.sql.SQLException;
import java.util.Arrays;

import ch.epfl.ad.AbstractQuery;
import ch.epfl.ad.db.parsing.Field;
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
		
		Field sId = new Field(s, "id");
		Field cId = new Field(c, "id");
		Field tSid = new Field(t, "sid");
		Field tCid = new Field(t, "cid");
		
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
				s,
				new Qualifier(Operator.NOT_EXISTS, new QueryRelation(
						cId,
						c,
						new Qualifier(Operator.NOT_EXISTS, new QueryRelation(
								tSid,
								t,
								Arrays.asList(
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
		Relation sId_S = new QueryRelation(sId, s, "myS");
		
		// (SELECT T.sid FROM T) myT
		Relation tSid_T = new QueryRelation(tSid, t, "myT");
		
		Field mySId = new Field(sId_S, "id");
		Field myTSid = new Field(tSid_T, "sid");
		
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
				Arrays.<Relation>asList(sId_S, tSid_T),
				new Qualifier(Operator.EQUALS, Arrays.<Operand>asList(mySId, myTSid))
				);
		
		System.out.println(query2);
		
		QueryGraph graph2 = new QueryGraph(query2);
		System.out.println(graph2);
		
		Relation p = new NamedRelation("P");
		Field pCid = new Field(p, "cid");
		
		// (SELECT T.sid, P.cid FROM P, T WHERE P.cid = C.id) myPT
		Relation tSidpCid_PT = new QueryRelation(
				Arrays.<Field>asList(tSid, pCid),
				Arrays.<Relation>asList(p, t),
				new Qualifier(Operator.EQUALS, Arrays.<Operand>asList(pCid, cId)),
				"myPT"
				);
		
		Field myPTSid = new Field(tSidpCid_PT, "sid");
		
		// SELECT S.id FROM S WHERE s.id = myPT.sid
		Relation sId_S2 = new QueryRelation(
				sId,
				s,
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
				c,
				new Qualifier(Operator.EXISTS, new QueryRelation(
						myPTSid,
						tSidpCid_PT,
						new Qualifier(Operator.EXISTS, sId_S2)
						))
				);
		
		System.out.println(query3);
		
		QueryGraph graph3 = new QueryGraph(query3);
		System.out.println(graph3);
	}
	
	public static void main(String[] args) throws SQLException, InterruptedException {
		new TestQuery().run(args);
	}
}
