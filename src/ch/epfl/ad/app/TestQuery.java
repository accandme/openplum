package ch.epfl.ad.app;

import java.sql.SQLException;
import java.util.Arrays;

import ch.epfl.ad.AbstractQuery;
import ch.epfl.ad.db.DatabaseManager;
import ch.epfl.ad.db.parsing.Field;
import ch.epfl.ad.db.parsing.NamedRelation;
import ch.epfl.ad.db.parsing.Operand;
import ch.epfl.ad.db.parsing.Operator;
import ch.epfl.ad.db.parsing.Qualifier;
import ch.epfl.ad.db.parsing.QueryRelation;
import ch.epfl.ad.db.parsing.Relation;

public class TestQuery extends AbstractQuery {

	@Override
	public void run(String[] args) throws SQLException, InterruptedException {
		
		if (args.length < 1) {
		    System.out.println("Arguments: config-file");
		    System.exit(1);
		}
		
		DatabaseManager dbManager = createDatabaseManager(args[0]);
		
		dbManager.setResultShipmentBatchSize(5000);
		
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
		
		// (SELECT S.id FROM S) myS
		Relation sId_S = new QueryRelation(sId, s, "myS");
		
		// (SELECT T.sid FROM T) myT
		Relation tSid_T = new QueryRelation(tSid, t, "myS");
		
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
	}
}
