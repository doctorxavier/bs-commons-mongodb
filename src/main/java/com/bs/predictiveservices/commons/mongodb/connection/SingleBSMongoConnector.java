package com.bs.predictiveservices.commons.mongodb.connection;

import java.net.UnknownHostException;

public class SingleBSMongoConnector extends BSMongoConnector {
	
	private static SingleBSMongoConnector instance = null;
	
	private SingleBSMongoConnector() {
		
	}
	
	public static SingleBSMongoConnector getInstance() throws UnknownHostException, MongoConnectorException {
		if(instance == null) {
			instance = new SingleBSMongoConnector();
			instance.authenticate().connect();
		}
		return instance;
	}

}
