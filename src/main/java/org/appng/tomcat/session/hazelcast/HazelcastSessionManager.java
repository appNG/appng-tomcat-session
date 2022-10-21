package org.appng.tomcat.session.hazelcast;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Arrays;
import java.util.HashSet;
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

/**
 * A {@link SessionManager} that uses Hazelcast's {@link IMap} for storing sessions.
 */
public class HazelcastSessionManager extends SessionManager<IMap<String, SessionData>> {

	private final Log log = Utils.getLog(HazelcastSessionManager.class);

	// Listen for sessions that have been removed from the persistent store
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
			// this must be done to avoid classloader issues, since the site
			// gets a new classloader when being reloaded
			long start = System.currentTimeMillis();
			byte[] data = message.getMessageObject();
			try {
				ByteArrayInputStream bais = new ByteArrayInputStream(data);
				ObjectInputStream ois = new ObjectInputStream(bais);
				final String siteName = (String) ois.readObject();
				bais.reset();
				ObjectInputStream siteOis = Utils.getObjectInputStream(bais, siteName, getContext());
				// org.appng.api.messaging.Serializer first writes the siteName, then the event itself
				siteOis.readObject();
				Object event = siteOis.readObject();
				String eventType = event.getClass().getName();
				if (clearSessionsOnEvent.contains(eventType)) {
					AtomicInteger count = new AtomicInteger();
					// @formatter:off
 					new HashSet<>(sessions.values())
						.parallelStream()
						.filter(s -> ((Session) s).getSite().equals(siteName))
						.forEach(s -> {
							count.incrementAndGet();
							sessions.remove(s.getId());
						});
 					// @formatter:on
					log.info(String.format("Received %s for site '%s' from %s, cleared %s local sessions in %sms!",
							eventType, siteName, message.getPublishingMember().getAddress(), count.get(),
							System.currentTimeMillis() - start));
				}
			} catch (ReflectiveOperationException c) {
				log.debug(String.format("Reading event caused %s: %s", c.getClass().getName(), c.getMessage()));
			} catch (Throwable t) {
				log.warn("Error processing event", t);
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
		// sufficient to use local keyset here, because every node runs this method
		Set<String> keys = new HashSet<>(getPersistentSessions().localKeySet());
		if (log.isDebugEnabled()) {
			log.debug(String.format("Performing expiration check for %s sessions.", keys.size()));
		}
		AtomicInteger count = new AtomicInteger(0);
		keys.forEach(id -> {
			try {
				// must not use a lock here!
				SessionData sessionData = getPersistentSessions().get(id);
				if (expireInternal(id, sessionData)) {
					count.incrementAndGet();
				}
			} catch (Throwable t) {
				log.error(String.format("Error expiring session %s", id), t);
			}
		});
		long timeEnd = System.currentTimeMillis();
		long duration = timeEnd - timeNow;
		processingTime += duration;
		if (log.isInfoEnabled()) {
			log.info(String.format("Expired %s of %s sessions in %sms", count, keys.size(), duration));
		}
		super.processExpires();
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
		if (log.isTraceEnabled()) {
			log.trace(String.format("Generated session ID: %s", result));
		}
		return result;
	}

	@Override
	// use a lock whilst finding the session
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
