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
		long start = System.currentTimeMillis();
		SessionData currentSessionData = null;
		if (hzSession.isDirty() || getPersistentSessions().get(session.getId())
				.checksum() != (currentSessionData = hzSession.serialize()).checksum()) {
			SessionData sessionData = null == currentSessionData ? hzSession.serialize() : currentSessionData;
			getPersistentSessions().set(session.getId(), sessionData);
			if (log.isDebugEnabled()) {
				log.debug(String.format("Saved %s in %dms", sessionData, System.currentTimeMillis() - start));
			}
			return true;

		}
		return false;
	}

	@Override
	public HazelcastSession findSession(String id) throws IOException {
		HazelcastSession session = (HazelcastSession) super.findSession(id);
		if (null == session) {

			long start = System.currentTimeMillis();

			// the calls are performed in a pessimistic lock block to prevent concurrency problems whilst finding
			// sessions
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
							log.debug(String.format("Loaded %s in %dms", sessionData,
									System.currentTimeMillis() - start));
						}
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
