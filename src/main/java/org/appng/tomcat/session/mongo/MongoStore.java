/*
 * Copyright 2015-2017 the original author or authors.
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.catalina.Container;
import org.apache.catalina.Context;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.Session;
import org.apache.catalina.Store;
import org.apache.catalina.session.StandardSession;
import org.apache.catalina.session.StoreBase;
import org.apache.juli.logging.Log;
import org.appng.tomcat.session.Utils;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadConcernLevel;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

/**
 * A {@link Store} implementation backed by MongoDB.
 */
public class MongoStore extends StoreBase {

	private final Log log = Utils.getLog(MongoStore.class);

	/** Property used to store the Session's ID */
	private static final String idProperty = "_id";

	/** Property used to store the Session's context name */
	protected static final String appContextProperty = "app";

	/** Property used to store the Session's last modified date. */
	protected static final String lastModifiedProperty = "lastModified";

	/** Property used to store the Session's creation date. */
	protected static final String creationTimeProperty = "creationTime";

	/** Property used to store the Session's data. */
	protected static final String sessionDataProperty = "data";

	/** Property used to store the name of the thread that loaded the session at last */
	private static final String THREAD_PROPERTY = "thread";

	/** Default Name of the Collection where the Sessions will be stored. */
	protected static final String sessionCollectionName = "tomcat.sessions";

	/** The descriptive information about this implementation. */
	protected static final String info = "MongoStore/1.0";

	/** Name to register for this Store, used for logging. */
	protected static String storeName = "MongoStore";

	/** Context or Web Application name associated with this Store */
	private String name = null;

	/** Name to register for the background thread. */
	protected String threadName = "MongoStore";

	/**
	 * MongoDB Connection URI. This will override all other connection settings specified. For more information, please
	 * see
	 * <a href="http://api.mongodb.org/java/current/com/mongodb/MongoClientURI.html">http://api.mongodb.org/java/current
	 * /com/mongodb/MongoClientURI.html</a>
	 * 
	 * @see MongoClientURI
	 */
	protected String connectionUri;

	/**
	 * MongoDB Hosts. This can be a single host or a comma separated list using a [host:port] format. Lists are
	 * typically used for replica sets. This value is ignored if the <em>dbConnectionUri</em> is provided.
	 * <p>
	 * 
	 * <pre>
	 * 		127.0.0.1:27001,127.0.0.1:27002
	 * </pre>
	 * 
	 * </p>
	 */
	protected String hosts;

	/** Name of the MongoDB Database to use. */
	protected String dbName;

	/** Name of the MongoDB Collection to store the sessions. Defaults to <em>tomcat.sessions</em> */
	protected String collectionName = sessionCollectionName;

	/** MongoDB User. Used if MongoDB is in <em>Secure</em> mode. */
	protected String username;

	/** MongoDB password. Used if MongoDB is in <em>Secure</em> mode */
	protected String password;

	/** Connection Timeout in milliseconds. Defaults to 0, or no timeout */
	protected int connectionTimeoutMs = 0;

	/** Connection Wait Timeout in milliseconds. Defaults to 0, or no timeout */
	protected int connectionWaitTimeoutMs = 0;

	/** Minimum Number of connections the MongoClient will manage. Defaults to 10. */
	protected int minPoolSize = 10;

	/** Maximum Number of connections the MongoClient will manage. Defaults to 20 */
	protected int maxPoolSize = 20;

	/** MongoDB replica set name. */
	protected String replicaSet;

	/** Time to Live for the data in Mongo */
	protected int timeToLive = -1;

	/** Controls if the MongoClient will use SSL. Defaults to false. */
	protected boolean useSecureConnection = false;

	/** Controls if the MongoClient will write to slaves. Equivalent to <em>slaveOk</em>. Defaults to false. */
	protected boolean useSlaves = false;

	/** Controls what {@link WriteConcern} the MongoClient will use. Defaults to {@link WriteConcern#MAJORITYs} */
	protected WriteConcern writeConcern = WriteConcern.MAJORITY;

	/** {@link MongoClient} instance to use. */
	protected MongoClient mongoClient;

	/** Mongo DB reference */
	protected DB db;

	/** Mongo Collection for the Sessions */
	protected DBCollection collection;

	/**
	 * The maximum time to wait when reading a session that is still used by another thread. 0 means: Don't wait at all.
	 */
	protected long maxWaitTime = 5000;

	/** The time to wait in one iteration when reading a session that is still used by another thread */
	protected long waitTime = 50;

	/** The {@link ReadPreference}, using {@link ReadPreference#primary()} for maximum consistency by default */
	protected ReadPreference readPreference = ReadPreference.primary();

	/** The {@link ReadConcern}, using {@link ReadConcern#MAJORITY} for maximum consistency by default */
	protected ReadConcern readConcern = ReadConcern.DEFAULT;

	/** Should a TTL index be used to expire sessions ? */
	private boolean useTTLIndex = false;

	/**
	 * Retrieve the unique Context name for this Manager. This will be used to separate out sessions from different
	 * application Contexts.
	 * 
	 * @return String unique name for this application Context
	 */
	protected String getName() {
		if (this.name == null) {
			Container container = this.manager.getContext();

			String contextName = container.getName();
			if (!contextName.startsWith("/")) {
				contextName = "/" + contextName;
			}

			String hostName = "";
			String engineName = "";

			if (container.getParent() != null) {
				Container host = container.getParent();
				hostName = host.getName();
				if (host.getParent() != null) {
					engineName = host.getParent().getName();
				}
			}

			this.name = "/" + engineName + "/" + hostName + contextName;
		}
		return this.name;
	}

	/**
	 * {@inheritDoc}
	 */
	public int getSize() throws IOException {
		Long count = this.collection.count(new BasicDBObject(appContextProperty, this.getName()));
		return count.intValue();
	}

	/**
	 * {@inheritDoc}
	 */
	public String[] keys() throws IOException {
		List<String> keys = new ArrayList<String>();

		BasicDBObject sessionKeyQuery = new BasicDBObject();
		sessionKeyQuery.put(appContextProperty, this.getName());

		DBCursor mongoSessionKeys = this.collection.find(sessionKeyQuery, new BasicDBObject(idProperty, 1));
		while (mongoSessionKeys.hasNext()) {
			String id = mongoSessionKeys.next().get(idProperty).toString();
			keys.add(id);
		}

		return keys.toArray(new String[keys.size()]);
	}

	@Override
	public void processExpires() {
		if (useTTLIndex) {
			debug("Session expiration is done by a TTL index");
		} else {
			int sessionTimeout = this.manager.getContext().getSessionTimeout();
			Date olderThan = new Date(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(sessionTimeout));
			BasicDBObject expireQuery = new BasicDBObject(lastModifiedProperty, new BasicDBObject("$lte", olderThan));
			DBCursor toExpire = this.collection.find(expireQuery);
			debug("Found %s sessions to expire with query: %s (older than %s)", toExpire.size(), expireQuery,
					olderThan);
			while (toExpire.hasNext()) {
				DBObject mongoSession = toExpire.next();
				String id = (String) mongoSession.get(idProperty);
				Long creationTime = (Long) mongoSession.get(creationTimeProperty);
				Date lastModified = (Date) mongoSession.get(lastModifiedProperty);
				long ageMinutes = TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis() - lastModified.getTime());
				debug("Expiring session %s created at %s, last accessed at %s (age: %smin)", id, new Date(creationTime),
						lastModified, ageMinutes);
				Session session = manager.createEmptySession();
				session.setId(id, false);
				session.setManager(manager);
				session.setCreationTime(creationTime);
				session.setValid(true);
				session.endAccess();
				// notify session listeners
				session.expire();
				this.collection.remove(mongoSession);
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	public StandardSession load(String id) throws ClassNotFoundException, IOException {
		long start = System.currentTimeMillis();
		StandardSession session = null;

		Container container = manager.getContext();
		Context context = (Context) container;

		ClassLoader appContextLoader = context.getLoader().getClassLoader();

		BasicDBObject sessionQuery = new BasicDBObject();
		sessionQuery.put(idProperty, id);
		sessionQuery.put(appContextProperty, this.getName());

		DBObject mongoSession = this.collection.findOne(sessionQuery);

		long waited = 0;
		while (waited < maxWaitTime && (null == mongoSession || (mongoSession.containsField(THREAD_PROPERTY)))) {
			if (null == mongoSession) {
				debug("Session %s has not (yet) been found.", id);
			} else {
				debug("Session %s is still used by Thread %s", id, mongoSession.get(THREAD_PROPERTY));
			}
			try {
				Thread.sleep(waitTime);
				waited += waitTime;
			} catch (InterruptedException e) {
				// ignore
			}
			mongoSession = this.collection.findOne(sessionQuery);
		}
		if (null == mongoSession) {
			throw new IOException(String.format("Failed to load session %s", id));
		}

		byte[] data = (byte[]) mongoSession.get(sessionDataProperty);

		if (data != null) {
			try (ObjectInputStream ois = Utils.getObjectInputStream(appContextLoader,
					manager.getContext().getServletContext(), data)) {

				session = (StandardSession) this.manager.createEmptySession();
				session.readObjectData(ois);
				session.setManager(this.manager);

				this.collection.update(sessionQuery, new BasicDBObject("$set",
						new BasicDBObject(THREAD_PROPERTY, Thread.currentThread().getName())));

				debug("Loaded session %s with query %s in %s ms (lastModified %s), owned by thread [%s]", id,
						sessionQuery, System.currentTimeMillis() - start, new Date(session.getLastAccessedTime()),
						Thread.currentThread().getName());
			} catch (ReflectiveOperationException e1) {
				throw new ClassNotFoundException("error loading session " + id, e1);
			}
		}

		return session;
	}

	/**
	 * {@inheritDoc}
	 */
	public void remove(String id) throws IOException {
		BasicDBObject sessionQuery = new BasicDBObject();
		sessionQuery.put(idProperty, id);
		sessionQuery.put(appContextProperty, this.getName());

		try {
			this.collection.remove(sessionQuery);
			debug("removed session %s (query: %s)", id, sessionQuery);
		} catch (MongoException e) {
			getLog().error("Unable to remove sessions for [" + id + ":" + this.getName() + "] from MongoDB", e);
			throw e;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	public void clear() throws IOException {
		BasicDBObject sessionQuery = new BasicDBObject();
		sessionQuery.put(appContextProperty, this.getName());

		try {
			this.collection.remove(sessionQuery);
			debug("removed sessions (query: %s)", sessionQuery);
		} catch (MongoException e) {
			getLog().error("Unable to remove sessions for [" + this.getName() + "] from MongoDB", e);
			throw e;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	public void save(Session session) throws IOException {
		long start = System.currentTimeMillis();

		try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(bos)) {

			((StandardSession) session).writeObjectData(oos);

			byte[] data = bos.toByteArray();

			BasicDBObject mongoSession = new BasicDBObject();
			mongoSession.put(idProperty, session.getIdInternal());
			mongoSession.put(appContextProperty, this.getName());
			mongoSession.put(creationTimeProperty, session.getCreationTime());
			mongoSession.put(sessionDataProperty, data);
			mongoSession.put(lastModifiedProperty, Calendar.getInstance().getTime());

			BasicDBObject sessionQuery = new BasicDBObject();
			sessionQuery.put(idProperty, session.getId());

			WriteResult updated = this.collection.update(sessionQuery, mongoSession, true, false);
			debug("Saved session %s with query %s in %s ms (lastModified %s) acknowledged: %s", session.getId(),
					sessionQuery, System.currentTimeMillis() - start, mongoSession.getDate(lastModifiedProperty),
					updated.wasAcknowledged());
		} catch (MongoException e) {
			getLog().error("Unable to save session to MongoDB", e);
			throw e;
		}
	}

	private void debug(String message, Object... args) {
		if (getLog().isDebugEnabled()) {
			getLog().debug(String.format(message, args));
		}
	}

	private void info(String message, Object... args) {
		if (getLog().isInfoEnabled()) {
			getLog().info(String.format(message, args));
		}
	}

	/**
	 * Initialize this Store by connecting to the MongoDB using the configuration parameters supplied.
	 */
	@Override
	protected void initInternal() {
		super.initInternal();
		try {
			this.getConnection();
		} catch (LifecycleException le) {
			throw new RuntimeException(le);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void destroyInternal() {
		super.destroyInternal();
		this.mongoClient.close();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected synchronized void startInternal() throws LifecycleException {
		super.startInternal();
		if (this.collection == null) {
			this.getConnection();
		}
	}

	@Override
	protected synchronized void stopInternal() throws LifecycleException {
		super.stopInternal();
		this.mongoClient.close();
	}

	/**
	 * Return the name for this Store, used for logging.
	 */
	@Override
	public String getStoreName() {
		return (storeName);
	}

	/**
	 * Create the {@link MongoClient}.
	 * 
	 * @throws LifecycleException
	 */
	private void getConnection() throws LifecycleException {
		try {
			if (this.connectionUri != null) {
				info("%s [%s]: Connecting to MongoDB [%s]", getStoreName(), this.getName(), this.connectionUri);
				this.mongoClient = new MongoClient(this.connectionUri);
			} else {
				if (this.useSlaves) {
					readPreference = ReadPreference.secondaryPreferred();
				}
				MongoClientOptions options = MongoClientOptions.builder().connectTimeout(connectionTimeoutMs)
						.maxWaitTime(connectionWaitTimeoutMs).connectionsPerHost(maxPoolSize).writeConcern(writeConcern)
						.readPreference(readPreference).readConcern(readConcern).build();

				List<ServerAddress> hosts = new ArrayList<ServerAddress>();
				String[] dbHosts = this.hosts.split(",");
				for (String dbHost : dbHosts) {
					String[] hostInfo = dbHost.split(":");
					ServerAddress address = new ServerAddress(hostInfo[0], Integer.parseInt(hostInfo[1]));
					hosts.add(address);
				}

				info("%s [%s]: Connecting to MongoDB [%s]", getStoreName(), this.getName(), this.hosts);

				List<MongoCredential> credentials = new ArrayList<MongoCredential>();
				if (this.username != null || this.password != null) {
					info("%s [%s]: Authenticating using [%s]", getStoreName(), this.getName(), this.username);
					for (int i = 0; i < hosts.size(); i++) {
						credentials.add(MongoCredential.createCredential(username, dbName, password.toCharArray()));
					}
				}
				this.mongoClient = new MongoClient(hosts, credentials, options);
			}

			info("%s [%s]: Using Database [%s]", getStoreName(), this.getName(), this.dbName);
			this.db = this.mongoClient.getDB(this.dbName);

			this.collection = this.db.getCollection(this.collectionName);
			info("%s [%s]: Preparing indexes", getStoreName(), this.getName());

			BasicDBObject lastModifiedIndex = new BasicDBObject(lastModifiedProperty, 1);
			try {
				this.collection.dropIndex(lastModifiedIndex);
				this.collection.dropIndex(new BasicDBObject(appContextProperty, 1));
			} catch (Exception e) {
				/* these indexes may not exist, so ignore */
			}

			this.collection.createIndex(new BasicDBObject(appContextProperty, 1));

			if (useTTLIndex) {
				BasicDBObject index = lastModifiedIndex;
				if (this.timeToLive != -1) {
					index = new BasicDBObject("expireAfterSeconds", this.timeToLive);
					this.collection.createIndex(lastModifiedIndex, index);
				} else {
					if (this.manager.getContext().getSessionTimeout() != -1) {
						index = new BasicDBObject("expireAfterSeconds",
								TimeUnit.MINUTES.toSeconds(this.manager.getContext().getSessionTimeout()));
						this.collection.createIndex(lastModifiedIndex, index);
					} else {
						this.collection.createIndex(lastModifiedIndex);
					}
				}
				info("%s [%s]: Created index [%s]", getStoreName(), this.getName(), index);
			}

			info("%s [%s]: Store ready.", getStoreName(), this.getName());
		} catch (MongoException me) {
			getLog().error("Unable to Connect to MongoDB", me);
			throw new LifecycleException(me);
		}
	}

	private Log getLog() {
		return log;
	}

	public void setConnectionUri(String connectionUri) {
		this.connectionUri = connectionUri;
	}

	public void setHosts(String hosts) {
		this.hosts = hosts;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public void setCollectionName(String collectionName) {
		this.collectionName = collectionName;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public void setConnectionTimeoutMs(int connectionTimeoutMs) {
		this.connectionTimeoutMs = connectionTimeoutMs;
	}

	public void setConnectionWaitTimeoutMs(int connectionWaitTimeoutMs) {
		this.connectionWaitTimeoutMs = connectionWaitTimeoutMs;
	}

	public void setMinPoolSize(int minPoolSize) {
		this.minPoolSize = minPoolSize;
	}

	public void setMaxPoolSize(int maxPoolSize) {
		this.maxPoolSize = maxPoolSize;
	}

	public void setReplicaSet(String replicaSet) {
		this.replicaSet = replicaSet;
	}

	public void setUseSecureConnection(boolean useSecureConnection) {
		this.useSecureConnection = useSecureConnection;
	}

	public void setUseSlaves(boolean useSlaves) {
		this.useSlaves = useSlaves;
	}

	public void setWriteConcern(String writeConcern) {
		this.writeConcern = WriteConcern.valueOf(writeConcern);
	}

	public void setTimeToLive(int timeToLive) {
		this.timeToLive = timeToLive;
	}

	public void setMaxWaitTime(long maxWaitTime) {
		this.maxWaitTime = maxWaitTime;
	}

	public void setWaitTime(long waitTime) {
		this.waitTime = waitTime;
	}

	public void setReadPreference(String readPreference) {
		this.readPreference = ReadPreference.valueOf(readPreference);
	}

	public void setReadConcern(String readConcern) {
		this.readConcern = new ReadConcern(ReadConcernLevel.valueOf(readConcern));
	}

}
