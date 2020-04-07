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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.Manager;
import org.apache.catalina.Session;
import org.apache.catalina.session.PersistentManagerBase;
import org.apache.catalina.session.StandardSession;
import org.apache.juli.logging.Log;
import org.appng.tomcat.session.Utils;

/**
 * A {@link Manager} implementation that uses a {@link MongoStore}
 */
public final class MongoPersistentManager extends PersistentManagerBase {

	private static final Log log = Utils.getLog(MongoPersistentManager.class);

	/**
	 * The descriptive information about this implementation.
	 */
	private static final String info = "MongoPersistentManager/1.0";

	/**
	 * The descriptive name of this Manager implementation (for logging).
	 */
	protected static String name = "MongoPersistentManager";

	/**
	 * Return descriptive information about this Manager implementation and the corresponding version number, in the
	 * format <code>&lt;description&gt;/&lt;version&gt;</code>.
	 */
	public String getInfo() {
		return (info);
	}

	/**
	 * Return the descriptive short name of this Manager implementation.
	 */
	@Override
	public String getName() {
		return (name);
	}

	@Override
	public MongoStore getStore() {
		return (MongoStore) super.getStore();
	}

	@Override
	public Session createSession(String sessionId) {
		Session session = super.createSession(sessionId);
		getStore().setSessionActive(session);
		try {
			save(session);
		} catch (IOException e) {
			log.warn(String.format("Error creating session: %s", session.getIdInternal()));
			session = null;
			getStore().setSessionInactive();
		}
		return session;
	}

	public void afterRequest() {
		getStore().setSessionInactive();
	}

	public void save(Session session) throws IOException {
		getStore().save(session);
	}

	@Override
	public void add(Session session) {
		// do nothing, we don't want to use Map<String,Session> sessions!
	}

	@Override
	public void remove(Session session, boolean update) {
		// avoid super.remove
		removeSession(session.getId());
	}

	@Override
	public Session findSession(String id) throws IOException {
		Session session = null;
		// do not call super, instead load the session directly from the store
		try {
			session = getStore().load(id);
		} catch (ClassNotFoundException e) {
			throw new IOException("error loading class for session " + id, e);
		}
		return session;
	}

	@Override
	public Session[] findSessions() {
		List<Session> sessions = new ArrayList<>();
		for (String id : getStore().keys()) {
			try {
				StandardSession session = getStore().loadNoLock(id);
				if (null != session) {
					sessions.add(session);
				}
			} catch (ClassNotFoundException | IOException e) {
				// ignore
			}
		}
		return sessions.toArray(new Session[0]);
	}

	@Override
	public void processPersistenceChecks() {
		// nothing to do since we don't swap in/out sessions
	}

	protected synchronized void startInternal() throws LifecycleException {
		log.info("[" + this.getName() + "]: Starting.");
		super.startInternal();
	}

	protected synchronized void stopInternal() throws LifecycleException {
		log.info("[" + this.getName() + "]: Stopping.");
		super.stopInternal();
	}

}
