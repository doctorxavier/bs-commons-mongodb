package com.bs.predictiveservices.commons.mongodb.constants.dbs;

public enum MonitoringDatabase {

	/**
	 * Collection enumeration
	 */
	MANUAL_TTL("manual_ttl");

	/**
	 * Database name.
	 */
	private static String DATABASE_NAME = "monitoring";
	private String id;

	private MonitoringDatabase(String id) {
		this.id = id;
	}

	public String getId() {
		return id;
	}

	public static String getDatabaseName() {
		return DATABASE_NAME;
	}
}