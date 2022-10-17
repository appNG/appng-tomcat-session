package org.appng.tomcat.session.hazelcast;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.LifecycleState;
import org.apache.juli.logging.Log;
import org.appng.tomcat.session.Session;
import org.appng.tomcat.session.SessionData;
import org.appng.tomcat.session.SessionManager;
import org.appng.tomcat.session.Utils;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.MessageListener;

public class HazelcastSessionManager extends SessionManager<IMap<String, SessionData>> {

	private final Log log = Utils.getLog(HazelcastSessionManager.class);

	// Listen for sessions that have been remove from the persistent store
	private class SessionRemovedListener implements EntryRemovedListener<String, SessionData> {
		@Override
		public void entryRemoved(EntryEvent<String, SessionData> event) {
			// remove locally cached session
			String sessionId = event.getOldValue().getId();
			org.apache.catalina.Session oldValue = sessions.remove(sessionId);
			if (log.isDebugEnabled()) {
				Address address = event.getMember().getAddress();
				if (null != oldValue) {
					log.debug(String.format("%s has been removed from local cache due to removal event from %s",
							sessionId, address));
				} else {
					log.debug(String.format("%s was not present in local cache, received removal event from %s",
							sessionId, address));
				}
			}
		}
	}

	private class SiteReloadListener implements MessageListener<byte[]> {
		@Override
		public void onMessage(Message<byte[]> message) {
			// clears the locally cached sessions when a SiteReloadEvent occurs
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
	}

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
		UUID listenerUid = getTopic().addMessageListener((MessageListener) new SiteReloadListener());
		log.info(String.format("Attached to topic %s with UUID %s", topicName, listenerUid));
		log.info(String.format("Loaded %s from %s", instance, configFile));
		log.info(String.format("Sticky: %s", sticky));
		setState(LifecycleState.STARTING);
		getPersistentSessions().addEntryListener(new SessionRemovedListener(), true);
	}

	protected ITopic<Object> getTopic() {
		return instance.getReliableTopic(topicName);
	}

	@Override
	public void processExpires() {
		long timeNow = System.currentTimeMillis();
		Set<String> keys;
		if (sticky) {
			keys = sessions.keySet();
		} else {
			keys = getPersistentSessions().keySet();
		}
		AtomicInteger count = new AtomicInteger(0);
		keys.forEach(k -> {
			SessionData sessionData = getPersistentSessions().get(k);
			try {
				Session session = Session.load(this, sessionData);
				if (null == session) {
					// session is not valid, so manager.remove(session, true) already has been called
					// which in turn will remove the session from the local cache and also from the persistent store
					count.incrementAndGet();
					if (log.isTraceEnabled()) {
						log.trace(String.format("%s has been removed by internal expiration", k, mapName));
					}
				}
			} catch (Exception e) {
				log.error("Error expiring session " + k, e);
			}
		});
		long timeEnd = System.currentTimeMillis();
		long duration = timeEnd - timeNow;
		processingTime += duration;
		if (log.isDebugEnabled()) {
			log.debug(String.format("Expired %s of %s sessions in %sms", count, keys.size(), duration));
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
	public void removeInternal(org.apache.catalina.Session session) {
		getPersistentSessions().remove(session.getId());
		if (log.isTraceEnabled()) {
			log.trace(String.format("%s has been removed from '%s'", session.getId(), mapName));
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
