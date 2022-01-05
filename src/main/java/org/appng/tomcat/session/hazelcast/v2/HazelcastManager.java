/*
 * Copyright 2015-2021 the original author or authors.
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
package org.appng.tomcat.session.hazelcast.v2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.LifecycleState;
import org.apache.catalina.Session;
import org.apache.catalina.session.ManagerBase;
import org.apache.juli.logging.Log;
import org.appng.tomcat.session.SessionData;
import org.appng.tomcat.session.Utils;
import org.appng.tomcat.session.hazelcast.HazelcastSession;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

public class HazelcastManager extends ManagerBase {

	private static final double NANOS_TO_MILLIS = 1000000d;
	private final Log log = Utils.getLog(HazelcastManager.class);
	private String configFile = "WEB-INF/classes/hazelcast.xml";
	private String mapName = "tomcat.sessions";
	private HazelcastInstance instance;

	@Override
	protected void startInternal() throws LifecycleException {
		super.startInternal();
		ClasspathXmlConfig config = new ClasspathXmlConfig(configFile);
		instance = Hazelcast.getOrCreateHazelcastInstance(config);
		log.info(String.format("Loaded %s from %s", instance, configFile));
		setState(LifecycleState.STARTING);
	}

	@Override
	protected void stopInternal() throws LifecycleException {
		super.stopInternal();
		setState(LifecycleState.STOPPING);
	}

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
	public HazelcastSession createEmptySession() {
		return new HazelcastSession(this);
	}

	@Override
	public HazelcastSession createSession(String sessionId) {
		return (HazelcastSession) super.createSession(sessionId);
	}

	boolean commit(Session session) throws IOException {
		return commit(session, null);
	}

	boolean commit(Session session, String alternativeSiteName) throws IOException {
		HazelcastSession hzSession = HazelcastSession.class.cast(session);
		hzSession.endAccess();
		long start = System.nanoTime();
		int oldChecksum = -1;
		boolean sessionDirty = false;
		if ((sessionDirty = hzSession.isDirty())
				|| (oldChecksum = getPersistentSessions().get(session.getId()).checksum()) != hzSession.checksum()) {
			SessionData sessionData = hzSession.serialize(alternativeSiteName);
			getPersistentSessions().set(session.getId(), sessionData);
			if (log.isDebugEnabled()) {
				String reason = sessionDirty ? "dirty-flag was set" : String.format("checksum <> %s", oldChecksum);
				log.debug(String.format(Locale.ENGLISH, "Saved %s (%s) in %.2fms", sessionData, reason,
						((System.nanoTime() - start)) / NANOS_TO_MILLIS));
			}
			return true;
		} else if (log.isTraceEnabled()) {
			log.trace(String.format("No changes in %s", session.getId()));
		}
		return false;
	}

	@Override
	public HazelcastSession findSession(String id) throws IOException {
		HazelcastSession session = (HazelcastSession) super.findSession(id);
		if (null == session) {
			long start = System.nanoTime();
			// the calls are performed in a pessimistic lock block to prevent
			// concurrency problems whilst finding sessions
			getPersistentSessions().lock(id);
			try {
				SessionData sessionData = getPersistentSessions().get(id);
				if (null == sessionData) {
					if (log.isDebugEnabled()) {
						log.debug(String.format("Session %s not found in map %s", id, mapName));
					}
				} else {
					try {
						session = HazelcastSession.create(this, sessionData);
						if (log.isDebugEnabled()) {
							log.debug(String.format(Locale.ENGLISH, "Loaded %s in %.2fms", sessionData,
									((System.nanoTime() - start)) / NANOS_TO_MILLIS));
						}
					} catch (ClassNotFoundException e) {
						log.error("Error loading session" + id, e);
					}
				}
			} finally {
				getPersistentSessions().unlock(id);
			}
		} else {
			session.access();
		}
		return session;
	}

	@Override
	public void add(Session session) {
		super.add(session);
		log.debug(String.format("Added %s", session.getId()));
	}

	/**
	 * Remove this Session from the active Sessions, but not from the distributed {@link IMap}.
	 *
	 * @param session
	 *                Session to be removed
	 */
	void removeLocal(Session session) {
		if (session.getIdInternal() != null) {
			sessions.remove(session.getIdInternal());
		}
	}

	@Override
	public void remove(Session session, boolean update) {
		super.remove(session, update);
		getPersistentSessions().remove(session.getId());
		log.debug(String.format("Removed %s (update: %s)", session.getId(), update));
	}

	IMap<String, SessionData> getPersistentSessions() {
		return instance.getMap(mapName);
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
