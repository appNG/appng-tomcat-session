/*
 * Copyright 2015-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.appng.tomcat.session.mongo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.LifecycleState;
import org.apache.catalina.Session;
import org.apache.juli.logging.Log;
import org.appng.tomcat.session.SessionData;
import org.appng.tomcat.session.SessionManager;
import org.appng.tomcat.session.Utils;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

/**
 * A {@link SessionManager} implementation that uses a {@link MongoClient}
 */
public final class MongoSessionManager extends SessionManager<DBCollection> {

	private final Log log = Utils.getLog(MongoSessionManager.class);

	protected static final String PROP_SESSIONDATA = "data";

	/** Property used to store the Session's ID */
	private static final String PROP_ID = "session_id";

	/** Property used to store the Session's context name */
	protected static final String PROP_CONTEXT = "app";

	/** Property used to store the Session's last modified date. */
	protected static final String PROP_LAST_MODIFIED = "lastModified";

	/** Property used to store the Session's creation date. */
	protected static final String PROP_CREATION_TIME = "creationTime";

	/** Mongo Collection for the Sessions */
	protected DBCollection collection;

	/** Name of the MongoDB Database to use. */
	protected String dbName = "tomcat_session";

	/** Name of the MongoDB Collection to store the sessions. Defaults to <em>tomcat.sessions</em> */
	protected String collectionName = "tomcat.sessions";

	/** {@link MongoClient} instance to use. */
	protected MongoClient mongoClient;

	/** Controls if the MongoClient will write to slaves. Equivalent to <em>slaveOk</em>. Defaults to false. */
	protected boolean useSlaves = false;

	/** The {@link ReadPreference}, using {@link ReadPreference#primary()} for maximum consistency by default */
	protected ReadPreference readPreference = ReadPreference.primary();

	/** The {@link ReadConcern}, using {@link ReadConcern#DEFAULT} for maximum consistency by default */
	protected ReadConcern readConcern = ReadConcern.DEFAULT;

	protected String connectionUri;

	/** MongoDB User. Used if MongoDB is in <em>Secure</em> mode. */
	protected String username;

	/** MongoDB password. Used if MongoDB is in <em>Secure</em> mode */
	protected String password;

	/** Connection Timeout in milliseconds. Defaults to 0, or no timeout */
	protected int connectionTimeoutMs = 0;

	/** Connection Wait Timeout in milliseconds. Defaults to 0, or no timeout */
	protected int connectionWaitTimeoutMs = 0;

	/** MongoDB replica set name. */
	protected String replicaSet;

	/** Sets whether writes should be retried if they fail due to a network error. */
	private boolean retryWrites = false;

	/** Controls what {@link WriteConcern} the MongoClient will use. Defaults to {@link WriteConcern#MAJORITYs} */
	protected WriteConcern writeConcern = WriteConcern.MAJORITY;

	/** Maximum Number of connections the MongoClient will manage. Defaults to 20 */
	protected int maxPoolSize = 20;

	/** The socket timeout when connecting to a MongoDB server */
	private int socketTimeout = 10000;

	/**
	 * Sets the server selection timeout in milliseconds, which defines how long the driver will wait for server
	 * selection to succeed before throwing an exception.
	 */
	private int serverSelectionTimeout = 30000;

	/**
	 * MongoDB Hosts. This can be a single host or a comma separated list using a [host:port] format. Lists are
	 * typically used for replica sets. This value is ignored if the <em>dbConnectionUri</em> is provided.
	 * <p>
	 * 
	 * <pre>
	 * 		127.0.0.1:27001,127.0.0.1:27002
	 * </pre>
	 * </p>
	 */
	protected String hosts;

	@Override
	public Log log() {
		return log;
	}

	@Override
	protected void startInternal() throws LifecycleException {
		super.startInternal();

		try {
			if (this.connectionUri != null) {
				log.info(String.format("Connecting to MongoDB [%s]", this.connectionUri));
				this.mongoClient = new MongoClient(this.connectionUri);
			} else {
				if (this.useSlaves) {
					readPreference = ReadPreference.secondaryPreferred();
				}

				// @formatter:off
				MongoClientOptions options = MongoClientOptions.builder()
					.connectTimeout(connectionTimeoutMs)
					.maxWaitTime(connectionWaitTimeoutMs)
					.connectionsPerHost(maxPoolSize)
					.writeConcern(writeConcern)
					.serverSelectionTimeout(serverSelectionTimeout)
					.retryWrites(retryWrites)
					.readPreference(readPreference)
					.readConcern(readConcern)
					.requiredReplicaSetName(replicaSet)
					.socketTimeout(socketTimeout)
					.build();
				// @formatter:on

				List<ServerAddress> hosts = new ArrayList<ServerAddress>();
				for (String dbHost : this.hosts.split(",")) {
					String[] hostInfo = dbHost.split(":");
					hosts.add(new ServerAddress(hostInfo[0], Integer.parseInt(hostInfo[1])));
				}

				log.info(String.format("Connecting to MongoDB [%s]", this.hosts));

				if (this.username != null || this.password != null) {
					log.info(String.format("Authenticating using [%s]", this.username));
					MongoCredential credential = MongoCredential.createCredential(username, dbName,
							password.toCharArray());
					this.mongoClient = new MongoClient(hosts, credential, options);
				} else {
					this.mongoClient = new MongoClient(hosts, options);
				}
			}

			log.info(String.format("Using Database [%s]", this.dbName));
			DB db = this.mongoClient.getDB(this.dbName);
			this.collection = db.getCollection(this.collectionName);
			log.info(String.format("Preparing indexes"));

			BasicDBObject lastModifiedIndex = new BasicDBObject(PROP_LAST_MODIFIED, 1);
			try {
				this.collection.dropIndex(lastModifiedIndex);
				this.collection.dropIndex(new BasicDBObject(PROP_CONTEXT, 1));
			} catch (Exception e) {
				/* these indexes may not exist, so ignore */
			}

			this.collection.createIndex(lastModifiedIndex);
			this.collection.createIndex(new BasicDBObject(PROP_CONTEXT, 1));
			log.info(String.format("[%s]: Store ready.", this.getName()));
		} catch (MongoException me) {
			log.error("Unable to Connect to MongoDB", me);
			throw new LifecycleException(me);
		}
		setState(LifecycleState.STARTING);
	}

	@Override
	protected void stopInternal() throws LifecycleException {
		super.stopInternal();
		this.mongoClient.close();
	}

	@Override
	protected SessionData findSessionInternal(String id) throws IOException {
		DBObject mongoSession = this.collection.findOne(sessionQuery(id));
		if (null != mongoSession) {
			byte[] sessionData = (byte[]) mongoSession.get(PROP_SESSIONDATA);
			try (ByteArrayInputStream bais = new ByteArrayInputStream(sessionData);
					ObjectInputStream ois = new ObjectInputStream(bais)) {
				return (SessionData) ois.readObject();
			} catch (ReflectiveOperationException roe) {
				log.error(String.format("Error loading session: %s", id), roe);
				throw new IOException(roe);
			}
		} else {
			log.warn(String.format("Session not found: %s, returning null!", id));
		}
		return null;
	}

	private BasicDBObject sessionQuery(String id) {
		return new BasicDBObject(PROP_ID, id).append(PROP_CONTEXT, this.getName());
	}

	@Override
	protected void updateSession(String id, SessionData sessionData) throws IOException {
		long start = System.nanoTime();

		try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(bos)) {

			oos.writeObject(sessionData);

			BasicDBObject sessionQuery = sessionQuery(sessionData.getId());
			BasicDBObject mongoSession = (BasicDBObject) sessionQuery.copy();
			mongoSession.put(PROP_SESSIONDATA, bos.toByteArray());
			mongoSession.put(PROP_LAST_MODIFIED, Calendar.getInstance().getTime());

			WriteResult updated = this.collection.update(sessionQuery, mongoSession, true, false);
			log.debug(String.format(Locale.ENGLISH,
					"Saved session %s with query %s in %.2fms (lastModified %s) acknowledged: %s", sessionData.getId(),
					sessionQuery, getDuration(start), mongoSession.getDate(PROP_LAST_MODIFIED),
					updated.wasAcknowledged()));
		} catch (MongoException | IOException e) {
			log.warn(String.format("Error saving session: %s", sessionData.getId()));
			throw e;
		}
	}

	@Override
	public void removeInternal(Session session) {
		String id = session.getId();
		BasicDBObject sessionQuery = sessionQuery(id);
		try {
			this.collection.remove(sessionQuery);
			log.debug(String.format("removed session %s (query: %s)", id, sessionQuery));
		} catch (MongoException e) {
			log.error("Unable to remove sessions for [" + id + ":" + this.getName() + "] from MongoDB", e);
			throw e;
		}
	}
	
	//@Override
	protected int expireInternal() {
		return 0;
	}

	public void setCollectionName(String collectionName) {
		this.collectionName = collectionName;
	}

	public void setConnectionUri(String connectionUri) {
		this.connectionUri = connectionUri;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	@Override
	protected DBCollection getPersistentSessions() {
		return collection;
	}
}
