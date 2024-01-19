package com.bs.predictiveservices.commons.mongodb.connection;

import java.net.UnknownHostException;
import java.util.Arrays;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;

public class MongoConnector {

	private static final String DEFAULT_SERVER = "localhost";
	private static final int DEFAULT_PORT = 27017;
	private static final ReadPreference DEFAULT_READ_PREFERENCE = ReadPreference.secondaryPreferred();
	private static final WriteConcern DEFAULT_WRITE_CONCERN = WriteConcern.UNACKNOWLEDGED;

	private String host;
	private Integer port;
	protected MongoClient client;
	private MongoCredential credential;
	private DB db;

	public MongoConnector() {
		this(DEFAULT_SERVER, DEFAULT_PORT);
	}

	public MongoConnector(String host) {
		this(host, DEFAULT_PORT);
	}

	public MongoConnector(String host, Integer port) {
		this.host = host;
		this.port = port;
		this.client = null;
		this.credential = null;
		this.db = null;
	}

	public MongoConnector authenticate(String user, String password, String database) {
		this.credential = MongoCredential.createMongoCRCredential(user, database, password.toCharArray());
		return this;
	}

	public MongoConnector connect() throws UnknownHostException, MongoConnectorException {
		return connect(DEFAULT_WRITE_CONCERN, DEFAULT_READ_PREFERENCE);
	}

	public MongoConnector connect(ReadPreference readPreference) throws UnknownHostException, MongoConnectorException {
		return connect(DEFAULT_WRITE_CONCERN, readPreference);
	}

	public MongoConnector connect(WriteConcern writeConcern) throws UnknownHostException, MongoConnectorException {
		return connect(writeConcern, DEFAULT_READ_PREFERENCE);
	}

	public MongoConnector connect(WriteConcern writeConcern, ReadPreference readPreference) throws UnknownHostException, MongoConnectorException {
		if (this.host == null || this.port == null) {
			throw new MongoConnectorException("No host and/or port selected.");
		}
		ServerAddress server = new ServerAddress(host, port);
		if (this.credential != null) {
			this.client = new MongoClient(server, Arrays.asList(this.credential));
		} else {
			this.client = new MongoClient(server);
		}
		this.client.setReadPreference(readPreference);
		this.client.setWriteConcern(writeConcern);
		return this;
	}

	public MongoConnector selectDatabase(String database) throws MongoConnectorException {
		if (this.client == null) {
			throw new MongoConnectorException("Not connected to any client.");
		}
		this.db = client.getDB(database);
		return this;
	}

	public DBCollection getCollection(String collection) throws MongoConnectorException {
		if (this.db == null) {
			throw new MongoConnectorException("Not connected to any database.");
		}
		return this.db.getCollection(collection);
	}

	public MongoConnector connectToDatabase(String database) throws UnknownHostException, MongoConnectorException {
		this.connect();
		this.selectDatabase(database);
		return this;
	}

	public MongoConnector connectToDatabase(String user, String password, String userDatabase, String database) throws UnknownHostException, MongoConnectorException {
		return this.authenticate(user, password, userDatabase).connectToDatabase(database);
	}

	public MongoConnector connectToServer(String host, int port) throws MongoConnectorException, UnknownHostException {
		if (this.client != null) {
			if (this.host.compareTo(host) != 0 || this.port != port) {
				throw new MongoConnectorException("MongoConnector already connected to another MongoDB instance: " + this.host + ":" + this.port + ". Trying " + host + " :" + port);
			} else {
				return this;
			}
		}
		this.host = host;
		this.port = port;
		return this.connect();
	}

	public void close() {
		if (this.db != null) {
			this.db = null;
		}
		if (this.credential != null) {
			this.credential = null;
		}
		if (this.client != null) {
			this.client.close();
			this.client = null;
		}
	}

}
