/*
 * Copyright 2015-2022 the original author or authors.
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
package org.appng.tomcat.session;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.LifecycleState;
import org.apache.catalina.session.ManagerBase;
import org.apache.juli.logging.Log;

public abstract class SessionManager<T> extends ManagerBase {

	private static final double NANOS_TO_MILLIS = 1000000d;

	protected boolean sticky = true;

	protected abstract void updateSession(String id, SessionData sessionData) throws IOException;

	protected abstract SessionData findSessionInternal(String id) throws IOException;

	public abstract void removeInternal(org.apache.catalina.Session session, boolean update);

	public abstract Log log();

	protected abstract T getPersistentSessions();

	// protected abstract int expireInternal();

	@Override
	protected void stopInternal() throws LifecycleException {
		super.stopInternal();
		setState(LifecycleState.STOPPING);
	}

	@Override
	public void processExpires() {
		long timeNow = System.currentTimeMillis();
		org.apache.catalina.Session sessions[] = findSessions();
		AtomicInteger expireHere = new AtomicInteger(0);
		Arrays.asList(sessions).stream().filter(s -> !(s == null || s.isValid()))
				.forEach(s -> expireHere.getAndIncrement());

//		int expiredInternal = expireInternal();
//		int expired = expireHere.addAndGet(expiredInternal);

		long duration = System.currentTimeMillis() - timeNow;
		if (log().isDebugEnabled()) {
			log().debug(String.format("Expired %d of %d sessions in %dms.", expireHere.intValue(), sessions.length,
					duration));
		}

		processingTime += duration;
	}

	@Override
	public Session createEmptySession() {
		return new Session(this);
	}

	@Override
	public Session createSession(String sessionId) {
		Session session = (Session) super.createSession(sessionId);
		if (log().isTraceEnabled()) {
			log().trace(String.format("Created %s (isNew: %s)", session.getId(), session.isNew()));
		}
		return session;
	}

	@Override
	public Session findSession(String id) throws IOException {
		long start = System.nanoTime();
		// Support multiple threads accessing the same, locally cached session.
		// Even if we're not sticky, this is OK, because SessionTrackerValve calls
		// manager.removeLocal(session) in that case!
		Session session = (Session) super.findSession(id);
		if (null == session) {
			SessionData sessionData = findSessionInternal(id);

			if (null == sessionData) {
				if (log().isDebugEnabled()) {
					log().debug(String.format("Session %s not found!", id));
				}
			} else {
				try {
					session = Session.create(this, sessionData);
					if (log().isDebugEnabled()) {
						log().debug(
								String.format(Locale.ENGLISH, "Loaded %s in %.2fms", sessionData, getDuration(start)));
					}
				} catch (ClassNotFoundException e) {
					log().error("Error loading session" + id, e);
				}
			}
		} else {
			if (log().isDebugEnabled()) {
				log().debug(
						String.format(Locale.ENGLISH, "Loaded %s from local store in %.2fms.", id, getDuration(start)));
			}
			session.access();
		}
		return session;
	}

	protected double getDuration(long nanoStart) {
		return ((System.nanoTime() - nanoStart)) / NANOS_TO_MILLIS;
	}

	public final boolean commit(Session session) throws IOException {
		return commit(session, null);
	}

	public final boolean commit(org.apache.catalina.Session session, String alternativeSiteName) throws IOException {
		long start = System.nanoTime();
		Session sessionInternal = Session.class.cast(session);
		session.endAccess();
		int oldChecksum = -1;
		boolean sessionDirty = false;
		boolean saved = false;
		if (!sticky || (sessionDirty = sessionInternal.isDirty())
				|| (oldChecksum = findSessionInternal(session.getId()).checksum()) != sessionInternal.checksum()) {
			SessionData sessionData = sessionInternal.serialize(alternativeSiteName);
			updateSession(sessionInternal.getId(), sessionData);
			saved = true;
			if (log().isDebugEnabled()) {
				String reason = sticky
						? (sessionDirty ? "dirty-flag was set" : String.format("checksum <> %s", oldChecksum))
						: "sticky=false";
				log().debug(String.format(Locale.ENGLISH, "Saved %s (%s) in %.2fms", sessionData, reason,
						getDuration(start)));
			}
		} else if (log().isDebugEnabled()) {
			log().debug(
					String.format("No changes in %s with checksum %s", session.getId(), sessionInternal.checksum()));
		}
		return saved;
	}

	@Override
	public void add(org.apache.catalina.Session session) {
		super.add(session);
		if (log().isTraceEnabled()) {
			log().trace(String.format("%s has been added to local cache", session.getId()));
		}
	}

	@Override
	public void remove(org.apache.catalina.Session session, boolean update) {
		super.remove(session, update);
		removeInternal(session, update);
	}

	/**
	 * Remove this Session from the active Sessions, but not from the persistent sessions
	 *
	 * @param session
	 *                Session to be removed
	 */
	public void removeLocal(org.apache.catalina.Session session) {
		if (session.getIdInternal() != null) {
			org.apache.catalina.Session removed = sessions.remove(session.getIdInternal());
			if (log().isTraceEnabled()) {
				String message = null == removed ? "%s was not locally cached."
						: "%s has been removed from local cache.";
				log().trace(String.format(message, session.getId()));
			}
		}
	}

	public boolean isSticky() {
		return sticky;
	}

	public void setSticky(boolean sticky) {
		this.sticky = sticky;
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
