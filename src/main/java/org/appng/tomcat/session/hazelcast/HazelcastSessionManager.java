package org.appng.tomcat.session.hazelcast;

import java.io.IOException;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.LifecycleState;
import org.apache.juli.logging.Log;
import org.appng.tomcat.session.SessionData;
import org.appng.tomcat.session.SessionManager;
import org.appng.tomcat.session.Utils;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

public class HazelcastSessionManager extends SessionManager<IMap<String, SessionData>> {

	private final Log log = Utils.getLog(HazelcastSessionManager.class);

	private String configFile = "hazelcast.xml";
	private String mapName = "tomcat.sessions";
	private HazelcastInstance instance;

	@Override
	public Log log() {
		return log;
	}

	@Override
	protected void startInternal() throws LifecycleException {
		super.startInternal();
		ClasspathXmlConfig config = new ClasspathXmlConfig(configFile);
		instance = Hazelcast.getOrCreateHazelcastInstance(config);
		log.info(String.format("Loaded %s from %s", instance, configFile));
		log.info(String.format("Always store session: %s", alwaysStoreSession));
		setState(LifecycleState.STARTING);
	}

	@Override
	protected void updateSession(String id, SessionData sessionData) {
		getPersistentSessions().set(id, sessionData);
	}

	@Override
	protected String generateSessionId() {
		String result = null;
		do {
			if (result != null) {
				// Not thread-safe but if one of multiple increments is lost
				// that is not a big deal since the fact that there was any
				// duplicate is a much bigger issue.
				duplicates++;
			}
			result = sessionIdGenerator.generateSessionId();
		} while (sessions.containsKey(result) || getPersistentSessions().containsKey(result));
		log.debug(String.format("Generated session ID: %s", result));
		return result;
	}

	@Override
	protected SessionData findSessionInternal(String id) throws IOException {
		SessionData sessionData = null;
		getPersistentSessions().lock(id);
		try {
			sessionData = getPersistentSessions().get(id);
		} finally {
			getPersistentSessions().unlock(id);
		}
		return sessionData;
	}

	@Override
	public void removeInternal(org.apache.catalina.Session session, boolean update) {
		getPersistentSessions().remove(session.getId());
		if (log.isTraceEnabled()) {
			log.trace(String.format("Removed %s (update: %s)", session.getId(), update));
		}
	}

	protected IMap<String, SessionData> getPersistentSessions() {
		return instance.getMap(mapName);
	}

	public void setConfigFile(String configFile) {
		this.configFile = configFile;
	}

	public void setMapName(String mapName) {
		this.mapName = mapName;
	}

}
