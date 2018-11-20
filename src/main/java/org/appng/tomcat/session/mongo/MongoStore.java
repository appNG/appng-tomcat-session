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
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;

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

	/** Controls what {@link WriteConcern} the MongoClient will use. Defaults to "ACKNOWLEDGED" */
	protected WriteConcern writeConcern = WriteConcern.ACKNOWLEDGED;

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

	// Tomcat's background thread that expires sessions
	private static final String CONTAINER_BACKGROUND_PROCESSOR = "ContainerBackgroundProcessor";

	/**
	 * Retrieve the unique Context name for this Manager. This will be used to separate out sessions from different
	 * application Contexts.
	 * 
	 * @return String unique name for this application Context
	 */
	protected String getName() {
		/* intialize the app name for this context */
		if (this.name == null) {
			/* get the container */
			Container container = this.manager.getContext();

			/* determine the context name from the container */
			String contextName = container.getName();
			if (!contextName.startsWith("/")) {
				contextName = "/" + contextName;
			}

			String hostName = "";
			String engineName = "";

			/* if this is a sub container, get the parent name */
			if (container.getParent() != null) {
				Container host = container.getParent();
				hostName = host.getName();
				if (host.getParent() != null) {
					engineName = host.getParent().getName();
				}
			}

			/* construct the unique context name */
			this.name = "/" + engineName + "/" + hostName + contextName;
		}
		/* return the name */
		return this.name;
	}

	/**
	 * {@inheritDoc}
	 */
	public int getSize() throws IOException {
		/* count the items in this collection for this app */
		Long count = this.collection.count(new BasicDBObject(appContextProperty, this.getName()));
		return count.intValue();
	}

	/**
	 * {@inheritDoc}
	 */
	public String[] keys() throws IOException {
		/* create the empty array list */
		List<String> keys = new ArrayList<String>();

		/* build the query */
		BasicDBObject sessionKeyQuery = new BasicDBObject();
		sessionKeyQuery.put(appContextProperty, this.getName());

		/* get the list */
		DBCursor mongoSessionKeys = this.collection.find(sessionKeyQuery, new BasicDBObject(idProperty, 1));
		while (mongoSessionKeys.hasNext()) {
			String id = mongoSessionKeys.next().get(idProperty).toString();
			keys.add(id);
		}

		/* return the array */
		return keys.toArray(new String[keys.size()]);
	}

	@Override
	public void processExpires() {
		getLog().info("processExpires");
		int sessionTimeout = this.manager.getContext().getSessionTimeout();
		long timeoutMillis = System.currentTimeMillis() - TimeUnit.MILLISECONDS.toMillis(sessionTimeout);
		BasicDBObject expireQuery = new BasicDBObject(lastModifiedProperty,
				new BasicDBObject("$lte", new Date(timeoutMillis)));
		DBCursor toExpire = this.collection.find(expireQuery);
		debug("Found %s sessions to expire with query: %s", toExpire.size(), expireQuery);
		while (toExpire.hasNext()) {
			DBObject session = toExpire.next();
			Object id = session.get(idProperty);
			Date lastModified = (Date) session.get(lastModifiedProperty);
			long ageMinutes = TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis() - lastModified.getTime());
			debug("Expiring session %s last accessed at %s (age: %sminutes)", id, lastModified, ageMinutes);
			this.collection.remove(session);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	public StandardSession load(String id) throws ClassNotFoundException, IOException {
		long start = System.currentTimeMillis();
		/* default session */
		StandardSession session = null;

		/* get a reference to the container */
		Container container = manager.getContext();
		Context context = (Context) container;

		/*
		 * store a reference to the old class loader, as we will change this thread's current context if we need to load
		 * custom classes
		 */
		Thread currentThread = Thread.currentThread();
		ClassLoader managerContextLoader = currentThread.getContextClassLoader();
		ClassLoader appContextLoader = context.getLoader().getClassLoader();

		/* locate the session, by id, in the collection */
		BasicDBObject sessionQuery = new BasicDBObject();
		sessionQuery.put(idProperty, id);
		sessionQuery.put(appContextProperty, this.getName());

		/* lookup the session */
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

		/* get the properties from mongo */
		byte[] data = (byte[]) mongoSession.get(sessionDataProperty);

		if (data != null) {
			currentThread.setContextClassLoader(appContextLoader);
			try (ObjectInputStream ois = Utils.getObjectInputStream(appContextLoader,
					manager.getContext().getServletContext(), data)) {

				/* create a new session */
				session = (StandardSession) this.manager.createEmptySession();
				session.readObjectData(ois);
				session.setManager(this.manager);

				if (!currentThread.getName().startsWith(CONTAINER_BACKGROUND_PROCESSOR)) {
					this.collection.update(sessionQuery,
							new BasicDBObject("$set", new BasicDBObject(THREAD_PROPERTY, currentThread.getName())));
					debug("Session %s is now owned by thread %s", id, currentThread.getName());
				}

				debug("Loaded session %s with query %s in %s ms (lastModified %s)", id, sessionQuery,
						System.currentTimeMillis() - start, new Date(session.getLastAccessedTime()));
			} catch (ReflectiveOperationException e1) {
				throw new ClassNotFoundException("error loading session " + id, e1);
			} finally {
				/* restore the class loader */
				currentThread.setContextClassLoader(managerContextLoader);
			}
		}

		/* return the session */
		return session;
	}

	/**
	 * {@inheritDoc}
	 */
	public void remove(String id) throws IOException {
		/* build up the query, looking for all sessions with this app context property and id */
		BasicDBObject sessionQuery = new BasicDBObject();
		sessionQuery.put(idProperty, id);
		sessionQuery.put(appContextProperty, this.getName());

		/* remove all sessions for this context and id */
		try {
			this.collection.remove(sessionQuery);
			debug("removed session %s (query: %s)", id, sessionQuery);
		} catch (MongoException e) {
			/* for some reason we couldn't remove the data */
			getLog().error("Unable to remove sessions for [" + id + ":" + this.getName() + "] from MongoDB", e);
			throw e;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	public void clear() throws IOException {
		/* build up the query, looking for all sessions with this app context property */
		BasicDBObject sessionQuery = new BasicDBObject();
		sessionQuery.put(appContextProperty, this.getName());

		/* remove all sessions for this context */
		try {
			this.collection.remove(sessionQuery);
			debug("removed sessions (query: %s)", sessionQuery);
		} catch (MongoException e) {
			/* for some reason we couldn't save the data */
			getLog().error("Unable to remove sessions for [" + this.getName() + "] from MongoDB", e);
			throw e;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	public void save(Session session) throws IOException {
		long start = System.currentTimeMillis();
		/*
		 * we will store the session data as a byte array in Mongo, so we need to set up our output streams
		 */
		try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(bos)) {

			/* serialize the session using the object output stream */
			((StandardSession) session).writeObjectData(oos);

			/* get the byte array of the data */
			byte[] data = bos.toByteArray();

			/* create the DBObject */
			BasicDBObject mongoSession = new BasicDBObject();
			mongoSession.put(idProperty, session.getIdInternal());
			mongoSession.put(appContextProperty, this.getName());
			mongoSession.put(creationTimeProperty, session.getCreationTime());
			mongoSession.put(sessionDataProperty, data);
			mongoSession.put(lastModifiedProperty, Calendar.getInstance().getTime());

			/* create our upsert lookup */
			BasicDBObject sessionQuery = new BasicDBObject();
			sessionQuery.put(idProperty, session.getId());

			/* update the object in the collection, inserting if necessary */
			this.collection.update(sessionQuery, mongoSession, true, false);
			debug("Saved session %s with query %s in %s ms (lastModified %s)", session.getId(), sessionQuery,
					System.currentTimeMillis() - start, mongoSession.getDate(lastModifiedProperty));
		} catch (MongoException e) {
			/* for some reason we couldn't save the data */
			getLog().error("Unable to save session to MongoDB", e);
			throw e;
		}
	}

	private void debug(String message, Object... args) {
		if (getLog().isDebugEnabled()) {
			getLog().debug(String.format(message, args));
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

		/* close the mongo client */
		this.mongoClient.close();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected synchronized void startInternal() throws LifecycleException {
		super.startInternal();

		/* verify that the collection reference is valid */
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
			/* create our MongoClient */
			if (this.connectionUri != null) {
				manager.getContext().getLogger().info(getStoreName() + "[" + this.getName()
						+ "]: Connecting to MongoDB [" + this.connectionUri + "]");
				this.mongoClient = new MongoClient(this.connectionUri);
			} else {
				/* create the client using the Mongo options */
				ReadPreference readPreference = ReadPreference.primaryPreferred();
				if (this.useSlaves) {
					readPreference = ReadPreference.secondaryPreferred();
				}
				MongoClientOptions options = MongoClientOptions.builder().connectTimeout(connectionTimeoutMs)
						.maxWaitTime(connectionWaitTimeoutMs).connectionsPerHost(maxPoolSize).writeConcern(writeConcern)
						.readPreference(readPreference).build();

				/* build up the host list */
				List<ServerAddress> hosts = new ArrayList<ServerAddress>();
				String[] dbHosts = this.hosts.split(",");
				for (String dbHost : dbHosts) {
					String[] hostInfo = dbHost.split(":");
					ServerAddress address = new ServerAddress(hostInfo[0], Integer.parseInt(hostInfo[1]));
					hosts.add(address);
				}

				getLog().info(getStoreName() + "[" + this.getName() + "]: Connecting to MongoDB [" + this.hosts + "]");

				/* connect */
				List<MongoCredential> credentials = new ArrayList<MongoCredential>();
				/* see if we need to authenticate */
				if (this.username != null || this.password != null) {
					getLog().info(
							getStoreName() + "[" + this.getName() + "]: Authenticating using [" + this.username + "]");
					for (int i = 0; i < hosts.size(); i++) {
						credentials.add(MongoCredential.createCredential(username, dbName, password.toCharArray()));
					}
				}
				this.mongoClient = new MongoClient(hosts, credentials, options);
			}

			/* get a connection to our db */
			getLog().info(getStoreName() + "[" + this.getName() + "]: Using Database [" + this.dbName + "]");
			this.db = this.mongoClient.getDB(this.dbName);

			/* get a reference to the collection */
			this.collection = this.db.getCollection(this.collectionName);
			getLog().info(getStoreName() + "[" + this.getName() + "]: Preparing indexes");

			/* drop any existing indexes */
			BasicDBObject lastModifiedIndex = new BasicDBObject(lastModifiedProperty, 1);
			try {
				this.collection.dropIndex(lastModifiedIndex);
				this.collection.dropIndex(new BasicDBObject(appContextProperty, 1));
			} catch (Exception e) {
				/* these indexes may not exist, so ignore */
			}

			/* make sure the last modified and app name indexes exists */
			this.collection.createIndex(new BasicDBObject(appContextProperty, 1));

			/* determine if we need to expire our db sessions */
			if (this.timeToLive != -1) {
				/* use the time to live set */
				this.collection.createIndex(lastModifiedIndex,
						new BasicDBObject("expireAfterSeconds", this.timeToLive));
			} else {
				/* no custom time to live specified, use the manager's settings */
				if (this.manager.getContext().getSessionTimeout() != -1) {
					/* create a ttl index on the app property */
					this.collection.createIndex(lastModifiedIndex, new BasicDBObject("expireAfterSeconds",
							TimeUnit.MINUTES.toSeconds(this.manager.getContext().getSessionTimeout())));
				} else {
					/* create a regular index */
					this.collection.createIndex(lastModifiedIndex);
				}
			}

			getLog().info(getStoreName() + "[" + this.getName() + "]: Store ready.");
		} catch (MongoException me) {
			getLog().error("Unable to Connect to MongoDB", me);
			throw new LifecycleException(me);
		}
	}

	private Log getLog() {
		return log;
	}

	public String getConnectionUri() {
		return connectionUri;
	}

	public void setConnectionUri(String connectionUri) {
		this.connectionUri = connectionUri;
	}

	public String getHosts() {
		return hosts;
	}

	public void setHosts(String hosts) {
		this.hosts = hosts;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public String getCollectionName() {
		return collectionName;
	}

	public void setCollectionName(String collectionName) {
		this.collectionName = collectionName;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public int getConnectionTimeoutMs() {
		return connectionTimeoutMs;
	}

	public void setConnectionTimeoutMs(int connectionTimeoutMs) {
		this.connectionTimeoutMs = connectionTimeoutMs;
	}

	public int getConnectionWaitTimeoutMs() {
		return connectionWaitTimeoutMs;
	}

	public void setConnectionWaitTimeoutMs(int connectionWaitTimeoutMs) {
		this.connectionWaitTimeoutMs = connectionWaitTimeoutMs;
	}

	public int getMinPoolSize() {
		return minPoolSize;
	}

	public void setMinPoolSize(int minPoolSize) {
		this.minPoolSize = minPoolSize;
	}

	public int getMaxPoolSize() {
		return maxPoolSize;
	}

	public void setMaxPoolSize(int maxPoolSize) {
		this.maxPoolSize = maxPoolSize;
	}

	public String getReplicaSet() {
		return replicaSet;
	}

	public void setReplicaSet(String replicaSet) {
		this.replicaSet = replicaSet;
	}

	public boolean isUseSecureConnection() {
		return useSecureConnection;
	}

	public void setUseSecureConnection(boolean useSecureConnection) {
		this.useSecureConnection = useSecureConnection;
	}

	public boolean isUseSlaves() {
		return useSlaves;
	}

	public void setUseSlaves(boolean useSlaves) {
		this.useSlaves = useSlaves;
	}

	public WriteConcern getWriteConcern() {
		return writeConcern;
	}

	public void setWriteConcern(WriteConcern writeConcern) {
		this.writeConcern = writeConcern;
	}

	public int getTimeToLive() {
		return timeToLive;
	}

	public void setTimeToLive(int timeToLive) {
		this.timeToLive = timeToLive;
	}

	public long getMaxWaitTime() {
		return maxWaitTime;
	}

	public void setMaxWaitTime(long maxWaitTime) {
		this.maxWaitTime = maxWaitTime;
	}

	public long getWaitTime() {
		return waitTime;
	}

	public void setWaitTime(long waitTime) {
		this.waitTime = waitTime;
	}

}
