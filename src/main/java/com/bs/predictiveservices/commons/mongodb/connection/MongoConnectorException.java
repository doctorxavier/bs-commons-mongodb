package com.bs.predictiveservices.commons.mongodb.connection;

public class MongoConnectorException extends Exception {

	private static final long serialVersionUID = -3250454550072779463L;

	public MongoConnectorException(String message) {
		super(message);
	}
	
	public MongoConnectorException(String message, Throwable throwable) {
		super(message, throwable);
	}
	
}
