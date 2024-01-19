package com.bs.predictiveservices.commons.mongodb.connection;

import com.bs.predictiveservices.commons.mongodb.constants.CollectionEnumeration;
import com.bs.predictiveservices.commons.mongodb.constants.DataBaseEnumeration;
import com.bs.predictiveservices.commons.mongodb.constants.MongoDBServerInfo;
import com.mongodb.DBCollection;

public class BSMongoConnector extends MongoConnector {

	public BSMongoConnector() {
		super(MongoDBServerInfo.SERVER, MongoDBServerInfo.PORT);
	}

	public MongoConnector authenticate() {
		return super.authenticate(MongoDBServerInfo.USER, MongoDBServerInfo.PASSWORD, MongoDBServerInfo.AUTH_DB);
	}

	public BSMongoConnector selectDatabase(DataBaseEnumeration database) throws MongoConnectorException {
		return (BSMongoConnector) super.selectDatabase(database.getId());
	}

	public DBCollection getCollection(CollectionEnumeration collection) throws MongoConnectorException {
		return super.getCollection(collection.getId());
	}

	public DBCollection getCollection(DataBaseEnumeration database, CollectionEnumeration collection) throws MongoConnectorException {
		return client.getDB(database.getId()).getCollection(collection.getId());
	}

}
