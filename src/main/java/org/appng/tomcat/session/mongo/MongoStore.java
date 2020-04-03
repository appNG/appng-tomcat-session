/*
 * Copyright 2015-2020 the original author or authors.
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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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

	/** Name of the Tomcat Thread that cleans up sessions in the background */
	private static final String TOMCAT_SESSION_THREAD = "ContainerBackgroundProcessor";

	/** The currently active session for this thread */
	protected ThreadLocal<Session> currentSession = new ThreadLocal<>();

	/** The ID of the currently active session for this thread */
	protected ThreadLocal<String> currentSessionId = new ThreadLocal<>();

	/** A map of currently used threads (ID -> thread-name) */
	protected ConcurrentMap<String, String> sessionsInUse = new ConcurrentHashMap<>();

	private final Log log = Utils.getLog(MongoStore.class);

	/** Property used to store the name of the host that loaded the session at last */
	private static final String HOST_PROPERTY = "host";

	/** Property used to store the Session's ID */
	private static final String idProperty = "session_id";

	/** Property used to store the Session's context name */
	protected static final String appContextProperty = "app";

	/** Property used to store the Session's last modified date. */
	protected static final String lastModifiedProperty = "lastModified";

	/** Property used to store the Session's creation date. */
	protected static final String creationTimeProperty = "creationTime";

	/** Property used to store the Session's data. */
	protected static final String sessionDataProperty = "data";

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

	/** The host name to use in {@value MongoStore#HOST_PROPERTY} */
	protected String hostName;

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
	protected long waitTime = 100;

	/** The {@link ReadPreference}, using {@link ReadPreference#primary()} for maximum consistency by default */
	protected ReadPreference readPreference = ReadPreference.primary();

	/** The {@link ReadConcern}, using {@link ReadConcern#DEFAULT} for maximum consistency by default */
	protected ReadConcern readConcern = ReadConcern.DEFAULT;

	/** Should a TTL index be used to expire sessions ? */
	private boolean useTTLIndex = false;

	/** The socket timeout when connecting to a MongoDB server */
	private int socketTimeout = 10000;

	/**
	 * Sets the server selection timeout in milliseconds, which defines how long the driver will wait for server
	 * selection to succeed before throwing an exception.
	 */
	private int serverSelectionTimeout = 30000;

	/** Sets whether writes should be retried if they fail due to a network error. */
	private boolean retryWrites = false;

	/** Are sessions sticky ? */
	private boolean sticky = false;

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
				Session session = manager.createEmptySession();
				session.setId(id, false);
				session.setManager(manager);
				session.setCreationTime(creationTime);
				session.setValid(true);
				session.endAccess();
				// notify session listeners
				session.expire();
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	public StandardSession load(String id) throws ClassNotFoundException, IOException {
		StandardSession session = null;

		if (null == id) {
			setSessionInactive();
		} else if (id.equals(currentSessionId.get())) {
			session = (StandardSession) currentSession.get();
			debug("Returning currently active session %s for thread %s", currentSessionId.get(),
					Thread.currentThread().getName());
		} else {

			BasicDBObject sessionQuery = sessionQuery(id);
			long start = System.currentTimeMillis();
			DBObject mongoSession = getMongoSession(id);

			String currentThread = Thread.currentThread().getName();
			if (null == mongoSession) {
				info("Session %s not found for thread %s (active session is %s), returning null!", id, currentThread,
						currentSessionId.get());
			} else {
				Container container = manager.getContext();
				Context context = (Context) container;
				ClassLoader appContextLoader = context.getLoader().getClassLoader();

				byte[] data = (byte[]) mongoSession.get(sessionDataProperty);
				if (data != null) {
					try (ObjectInputStream ois = Utils.getObjectInputStream(appContextLoader,
							manager.getContext().getServletContext(), data)) {

						session = (StandardSession) this.manager.createEmptySession();
						session.readObjectData(ois);
						session.setManager(this.manager);

						if (!(sticky || currentThread.startsWith(TOMCAT_SESSION_THREAD))) {
							BasicDBObject setHost = new BasicDBObject("$set", new BasicDBObject(HOST_PROPERTY,
									String.format("%s@%s", Thread.currentThread().getName(), hostName)));
							this.collection.update(sessionQuery, setHost);
						}
						debug("Loaded session %s with query %s in %s ms (lastModified %s)", id, sessionQuery,
								System.currentTimeMillis() - start, new Date(session.getLastAccessedTime()));

						setSessionActive(session);
					} catch (ReflectiveOperationException roe) {
						getLog().error(String.format("Error loading session: %s", id), roe);
						throw roe;
					}
				} else {
					warn("No data for session: %s, returning null!", id);
					setSessionInactive();
				}
			}
		}
		return session;
	}

	private DBObject getMongoSession(String id) {

		int waited = 0;
		DBObject mongoSession;
		BasicDBObject sessionQuery = sessionQuery(id);
		if (sticky) {
			String owningThread = null;
			String threadName = Thread.currentThread().getName();
			if (!threadName.startsWith(TOMCAT_SESSION_THREAD)) {
				while (waited < maxWaitTime && (owningThread = sessionsInUse.get(id)) != null
						&& !owningThread.equals(threadName)) {
					info("Session %s is still used by thread %s, waiting %sms.", id, owningThread, waitTime);
					waited = doWait(waited);
				}
			}
			mongoSession = this.collection.findOne(sessionQuery);
		} else {
			mongoSession = this.collection.findOne(sessionQuery);
			if (null == mongoSession) {
				return null;
			}
			while (waited < maxWaitTime && mongoSession.get(HOST_PROPERTY) != null) {
				info("Session %s is still used by host %s, waiting %sms.", id, mongoSession.get(HOST_PROPERTY),
						waitTime);
				waited = doWait(waited);
				mongoSession = this.collection.findOne(sessionQuery);
			}
		}
		return mongoSession;
	}

	private int doWait(int waited) {
		try {
			Thread.sleep(waitTime);
		} catch (InterruptedException e) {
			// ignore
		}
		return waited + (int) waitTime;
	}

	public void setSticky(String sticky) {
		this.sticky = "true".equalsIgnoreCase(sticky);
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	private BasicDBObject sessionQuery(String id) {
		return new BasicDBObject(idProperty, id).append(appContextProperty, this.getName());
	}

	/**
	 * {@inheritDoc}
	 */
	public void remove(String id) throws IOException {
		BasicDBObject sessionQuery = sessionQuery(id);
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

			BasicDBObject sessionQuery = sessionQuery(session.getIdInternal());
			BasicDBObject mongoSession = (BasicDBObject) sessionQuery.copy();
			mongoSession.put(creationTimeProperty, session.getCreationTime());
			mongoSession.put(sessionDataProperty, data);
			mongoSession.put(lastModifiedProperty, Calendar.getInstance().getTime());

			WriteResult updated = this.collection.update(sessionQuery, mongoSession, true, false);
			debug("Saved session %s with query %s in %s ms (lastModified %s) acknowledged: %s", session.getId(),
					sessionQuery, System.currentTimeMillis() - start, mongoSession.getDate(lastModifiedProperty),
					updated.wasAcknowledged());
		} catch (MongoException e) {
			warn("Error saving session: %s", session.getIdInternal());
			throw e;
		}
	}

	private void debug(String message, Object... args) {
		if (getLog().isDebugEnabled()) {
			getLog().debug(String.format(message, args));
		}
	}

	private void trace(String message, Object... args) {
		if (getLog().isTraceEnabled()) {
			getLog().trace(String.format(message, args));
		}
	}

	private void info(String message, Object... args) {
		if (getLog().isInfoEnabled()) {
			getLog().info(String.format(message, args));
		}
	}

	private void warn(String message, Object... args) {
		if (getLog().isWarnEnabled()) {
			getLog().warn(String.format(message, args));
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
		if (null == hostName) {
			try {
				this.hostName = InetAddress.getLocalHost().getHostName();
			} catch (UnknownHostException e) {
				throw new LifecycleException(e);
			}
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
						.serverSelectionTimeout(serverSelectionTimeout).retryWrites(retryWrites)
						.readPreference(readPreference).readConcern(readConcern).requiredReplicaSetName(replicaSet)
						.socketTimeout(socketTimeout).build();

				List<ServerAddress> hosts = new ArrayList<ServerAddress>();
				for (String dbHost : this.hosts.split(",")) {
					String[] hostInfo = dbHost.split(":");
					hosts.add(new ServerAddress(hostInfo[0], Integer.parseInt(hostInfo[1])));
				}

				info("%s [%s]: Connecting to MongoDB [%s]", getStoreName(), this.getName(), this.hosts);

				if (this.username != null || this.password != null) {
					info("%s [%s]: Authenticating using [%s]", getStoreName(), this.getName(), this.username);
					MongoCredential credential = MongoCredential.createCredential(username, dbName,
							password.toCharArray());
					this.mongoClient = new MongoClient(hosts, credential, options);
				} else {
					this.mongoClient = new MongoClient(hosts, options);
				}
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

	void setSessionInactive() {
		String id = currentSessionId.get();
		debug("Removing session from thread %s: %s", Thread.currentThread().getName(), id);
		currentSession.remove();
		currentSessionId.remove();
		if (id != null) {
			sessionsInUse.remove(id);
		}
	}

	void setSessionActive(Session session) {
		currentSession.set(session);
		currentSessionId.set(session.getId());
		sessionsInUse.put(session.getId(), Thread.currentThread().getName());
		debug("Setting session for thread %s: %s", Thread.currentThread().getName(), session.getId());
	}

	// Setters
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

	public void setServerSelectionTimeout(int serverSelectionTimeout) {
		this.serverSelectionTimeout = serverSelectionTimeout;
	}

	public void setRetryWrites(boolean retryWrites) {
		this.retryWrites = retryWrites;
	}
}
