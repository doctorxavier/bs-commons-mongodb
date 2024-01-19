package com.bs.predictiveservices.commons.mongodb.collection.ttl;

import java.net.UnknownHostException;
import java.util.Calendar;
import java.util.Date;

import com.bs.predictiveservices.commons.mongodb.connection.MongoConnector;
import com.bs.predictiveservices.commons.mongodb.connection.MongoConnectorException;
import com.bs.predictiveservices.commons.mongodb.constants.MongoDBServerInfo;
import com.bs.predictiveservices.commons.mongodb.constants.dbs.MonitoringDatabase;
import com.bs.predictiveservices.commons.mongodb.document.QueryConstructor;
import com.mongodb.DBCollection;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

public class ManualTtl {

	private static final int NUM_PARAMS = 4;
	private static final int DATABASE = 0;
	private static final int COLLECTION = 1;
	private static final int TTL_FIELD = 2;
	private static final int TTL = 3;

	private static final String MONITORING_DATABASE = MonitoringDatabase.getDatabaseName();
	private static final String MANUAL_TTL_COLLECTION = MonitoringDatabase.MANUAL_TTL.getId();

	public static void main(String[] args) throws MongoConnectorException, UnknownHostException {
		// Parsing arguments
		if (args.length != NUM_PARAMS) {
			System.out.println("Invalid number of params: " + args.length);
			System.out.println("Expected params: <database> <collection> <ttl field> <ttl (days)>");
			System.exit(-1);
		}
		final String databaseName = args[DATABASE];
		final String collectionName = args[COLLECTION];
		final String ttlFieldName = args[TTL_FIELD];
		final int ttl = Integer.parseInt(args[TTL]);
		final Calendar ttlDate = Calendar.getInstance();
		ttlDate.add(Calendar.DAY_OF_YEAR, -ttl);

		// Starting process monitoring
		final Date initDate = Calendar.getInstance().getTime();

		// Connecting to MongoDB
		final MongoConnector mongo = new MongoConnector(MongoDBServerInfo.SERVER, MongoDBServerInfo.PORT).authenticate(MongoDBServerInfo.USER, MongoDBServerInfo.PASSWORD,
				MongoDBServerInfo.AUTH_DB).connect();
		final DBCollection collection = mongo.selectDatabase(databaseName).getCollection(collectionName);
		final DBCollection manualTtlCollection = mongo.selectDatabase(MONITORING_DATABASE).getCollection(MANUAL_TTL_COLLECTION);
		final QueryConstructor c = new QueryConstructor();

		// Removing documents
		final WriteResult result = collection.remove(c.field(ttlFieldName, c.lt(ttlDate.getTime())), WriteConcern.ACKNOWLEDGED);

		// Stop process monitoring and writing results
		manualTtlCollection.insert(c.document(c.field("init", initDate), c.field("end", Calendar.getInstance().getTime()), c.field("col", databaseName + "." + collectionName),
				c.field("docs", result.getN()), c.field("modified", new Date()), c.field("ttl", ttlDate.getTime()), c.field("field", ttlFieldName)));

		// Closing connections
		mongo.close();
	}
}
