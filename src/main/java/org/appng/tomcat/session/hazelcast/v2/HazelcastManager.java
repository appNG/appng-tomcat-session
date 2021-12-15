package org.appng.tomcat.session.hazelcast.v2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.Session;
import org.apache.catalina.session.ManagerBase;
import org.apache.catalina.session.StandardSession;
import org.apache.juli.logging.Log;
import org.appng.tomcat.session.SessionData;
import org.appng.tomcat.session.Utils;
import org.appng.tomcat.session.hazelcast.HazelCastSession;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

public class HazelcastManager extends ManagerBase {

	private final Log log = Utils.getLog(HazelcastManager.class);
	private String configFile = "WEB-INF/classes/hazelcast.xml";
	private String mapName = "tomcat.sessions";
	private HazelcastInstance instance;

	@Override
	public void processExpires() {
		long timeNow = System.currentTimeMillis();
		Session sessions[] = findSessions();
		AtomicInteger expireHere = new AtomicInteger(0);
		Arrays.asList(sessions).stream().filter(s -> !(s == null || s.isValid()))
				.forEach(s -> expireHere.getAndIncrement());
		long duration = System.currentTimeMillis() - timeNow;
		if (log.isDebugEnabled()) {
			log.debug(String.format("Expired %d of %d sessions in %dms.", expireHere.intValue(), sessions.length,
					duration));
		}
		processingTime += duration;
	}

	@Override
	protected void initInternal() throws LifecycleException {
		instance = Hazelcast.getOrCreateHazelcastInstance(new ClasspathXmlConfig(configFile));
		log.info(String.format("Loaded %s from %s", instance, configFile));
		log.info("Using classpath config:" + getClass().getClassLoader().getResource(configFile));
	}

	@Override
	public HazelCastSession createEmptySession() {
		return new HazelCastSession(this);
	}

	@Override
	public HazelCastSession createSession(String sessionId) {
		return (HazelCastSession) super.createSession(sessionId);
	}

	public void commit(Session session) throws IOException {
		if (((HazelCastSession) session).isDirty()) {
			try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
					ObjectOutputStream oos = new ObjectOutputStream(bos)) {
				((HazelCastSession) session).setClean();
				((HazelCastSession) session).writeObjectData(oos);
				String site = Utils.getSiteName(session.getSession());
				SessionData sessionData = new SessionData(session.getId(), site, bos.toByteArray());
				getPersistentSessions().set(session.getId(), sessionData);
				log.debug("saved: " + sessionData + " with TTL of " + session.getMaxInactiveInterval() + " seconds");
			}
		}
	}

	@Override
	public HazelCastSession findSession(String id) throws IOException {
		HazelCastSession session = (HazelCastSession) super.findSession(id);
		if (null == session) {

			// the calls are performed in a pessimistic lock block to prevent concurrency problems whilst finding
			// sessions
			getPersistentSessions().lock(id);
			try {
				SessionData sessionData = getPersistentSessions().get(id);
				if (null == sessionData) {
					log.debug(String.format("Session %s not found in map %s", id, mapName));
				} else {
					try (ByteArrayInputStream is = new ByteArrayInputStream(sessionData.getData());
							ObjectInputStream ois = Utils.getObjectInputStream(is, sessionData.getSite(),
									getContext())) {
						session = createEmptySession();
						((StandardSession) session).readObjectData(ois);
						session.access();
						session.endAccess();
						session.setClean();
						log.debug("loaded: " + sessionData);
						add(session);
					} catch (ClassNotFoundException e) {
						log.error("Error loading session" + id, e);
					}
				}
			} finally {
				getPersistentSessions().unlock(id);
			}
		} else {
			session.access();
			session.endAccess();
		}
		return session;
	}

	@Override
	public void add(Session session) {
		super.add(session);
		log.debug(String.format("Added %s", session.getId()));
	}

	void removeLocal(Session session) {
		super.remove(session, true);
	}

	@Override
	public void remove(Session session, boolean update) {
		super.remove(session, update);
		getPersistentSessions().remove(session.getId());
		log.debug(String.format("Removed %s (update: %s)", session.getId(), update));
	}

	private IMap<String, SessionData> getPersistentSessions() {
		return instance.getMap(mapName);
	}

	int getPersistentSize() {
		return getPersistentSessions().size();
	}

	public void setConfigFile(String configFile) {
		this.configFile = configFile;
	}

	public void setMapName(String mapName) {
		this.mapName = mapName;
	}

	@Override
	public void load() throws ClassNotFoundException, IOException {
		// don't load all sessions when starting
	}

	@Override
	public void unload() throws IOException {
		// don't save all sessions when stopping
	}

}
