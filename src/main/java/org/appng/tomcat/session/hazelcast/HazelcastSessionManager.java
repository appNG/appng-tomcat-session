package org.appng.tomcat.session.hazelcast;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

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
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.MessageListener;

public class HazelcastSessionManager extends SessionManager<IMap<String, SessionData>>
		implements MessageListener<byte[]> {

	private final Log log = Utils.getLog(HazelcastSessionManager.class);

	private String configFile = "hazelcast.xml";
	private String mapName = "tomcat.sessions";
	private String topicName = "appng-messaging";
	protected List<String> clearSessionsOnEvent = Arrays.asList("org.appng.core.controller.messaging.ReloadSiteEvent",
			"org.appng.appngizer.controller.SiteController.ReloadSiteFromAppNGizer");
	private HazelcastInstance instance;

	@Override
	public Log log() {
		return log;
	}

	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected void startInternal() throws LifecycleException {
		super.startInternal();
		ClasspathXmlConfig config = new ClasspathXmlConfig(configFile);
		instance = Hazelcast.getOrCreateHazelcastInstance(config);
		UUID listenerUid = getTopic().addMessageListener((MessageListener) this);
		log.info(String.format("Attached to topic %s with UUID %s", topicName, listenerUid));
		log.info(String.format("Loaded %s from %s", instance, configFile));
		log.info(String.format("Sticky: %s", sticky));
		setState(LifecycleState.STARTING);
	}

	protected ITopic<Object> getTopic() {
		return instance.getReliableTopic(topicName);
	}

	@Override
	public void onMessage(Message<byte[]> message) {
		byte[] data = message.getMessageObject();
		try {
			ByteArrayInputStream bais = new ByteArrayInputStream(data);
			ObjectInputStream ois = new ObjectInputStream(bais);
			String siteName = (String) ois.readObject();
			bais.reset();
			ObjectInputStream siteOis = Utils.getObjectInputStream(bais, siteName, getContext());
			// org.appng.api.messaging.Serializer first writes the siteName, then the event itself
			siteName = (String) siteOis.readObject();
			Object event = siteOis.readObject();
			String eventType = event.getClass().getName();
			if (clearSessionsOnEvent.contains(eventType)) {
				log.info(String.format("Received %s, clearing %s local sessions!", eventType, sessions.size()));
				sessions.clear();
			}
		} catch (Throwable t) {
			log.error("Error reading event", t);
		}
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

	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}

}
