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
import java.util.Date;
import java.util.Locale;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.LifecycleState;
import org.apache.catalina.session.ManagerBase;
import org.apache.juli.logging.Log;

/**
 * Basic implementation of {@link ManagerBase} using a persistent storage.
 * 
 * @param <T>
 *            the type of the persistent storage
 */
public abstract class SessionManager<T> extends ManagerBase {

	private static final double NANOS_TO_MILLIS = 1000000d;

	protected boolean sticky = true;

	protected int sessionSaveIntervalSeconds = 10;

	/**
	 * Updates the session in the persistent storage.
	 * 
	 * @param id
	 *                    the id of the session
	 * @param sessionData
	 *                    the session's {@link SessionData}
	 * 
	 * @throws IOException
	 *                     if an error occurs while updating the session
	 */
	protected abstract void updateSession(String id, SessionData sessionData) throws IOException;

	/**
	 * Retrieves the {@link SessionData} from the persistent storage
	 * 
	 * @param id
	 *           the id of the session
	 * 
	 * @return the {@link SessionData} of the session, or {@code null} if the session could not be found
	 * 
	 * @throws IOException
	 *                     if an error occurs while retrieving the data
	 */
	protected abstract SessionData findSessionInternal(String id) throws IOException;

	/**
	 * Remove the session from the underlying persistent storage
	 * 
	 * @param session
	 *                the session to remove
	 */
	public abstract void removeInternal(org.apache.catalina.Session session);

	public abstract Log log();

	/**
	 * Returns the persistent storage.
	 * 
	 * @return the persistent storage
	 */
	protected abstract T getPersistentSessions();

	@Override
	protected void stopInternal() throws LifecycleException {
		super.stopInternal();
		setState(LifecycleState.STOPPING);
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
					session = Session.load(this, sessionData);
					if (log().isDebugEnabled()) {
						if (null == session) {
							log().debug(String.format("Session %s found, but has expired!", id));
						} else {
							log().debug(String.format(Locale.ENGLISH, "Loaded %s in %.2fms", sessionData,
									getDuration(start)));
						}
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

	/**
	 * If one of these condition matches, the session is persisted to the store:
	 * <ul>
	 * <li>Is this manager non-sticky?
	 * <li>Is the session marked as dirty?
	 * <li>Is the session's new checksum different from the old checksum?
	 * <li>Is it more than {@code sessionSaveIntervalSeconds} seconds ago when the session was last accessed?
	 * </ul>
	 *
	 * @param session
	 * @param alternativeSiteName
	 * 
	 * @return
	 * 
	 * @throws IOException
	 */
	public final boolean commit(org.apache.catalina.Session session, String alternativeSiteName) throws IOException {
		session.endAccess();
		boolean saved = false;
		long start = System.nanoTime();

		SessionData oldSessionData = null;
		Session sessionInternal = Session.class.cast(session);
		boolean sessionDirty = sessionInternal.isDirty();
		int checksum = -1;
		int oldChecksum = -1;
		long secondsSinceAccess = 0;
		if (!sticky || sessionDirty || (checksum = sessionInternal
				.checksum()) != (oldChecksum = ((oldSessionData = findSessionInternal(session.getId())).checksum()))
				|| (secondsSinceAccess = oldSessionData.secondsSinceAccessed()) > sessionSaveIntervalSeconds) {
			SessionData sessionData = sessionInternal.serialize(alternativeSiteName);
			updateSession(sessionInternal.getId(), sessionData);
			saved = true;
			if (log().isDebugEnabled()) {
				String reason = sticky
						? (sessionDirty ? "dirty-flag was set"
								: (secondsSinceAccess > sessionSaveIntervalSeconds
										? "last accessed " + secondsSinceAccess + " seconds ago"
										: String.format("checksum %s <> %s", oldChecksum, checksum)))
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
			log().trace(String.format("%s has been added to local cache.", session.getId()));
		}
	}

	@Override
	public void remove(org.apache.catalina.Session session, boolean expired) {
		super.remove(session, expired);
		removeInternal(session);
		if (expired && log().isDebugEnabled()) {
			String message = String.format(Locale.ENGLISH,
					"%s has expired! Last accessed at %s, maxLifeTime: %ss, age: %s seconds", session.getId(),
					new Date(session.getLastAccessedTimeInternal()), session.getMaxInactiveInterval(),
					(System.currentTimeMillis() - session.getLastAccessedTimeInternal()) / 1000);
			log().debug(message);
		}
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

	public void setSessionSaveIntervalSeconds(int sessionSaveIntervalSeconds) {
		this.sessionSaveIntervalSeconds = sessionSaveIntervalSeconds;
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
