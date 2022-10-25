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

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.security.Principal;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import org.apache.catalina.Manager;

/**
 * A {@link Session} that can be flagged as dirty.
 */
public class Session extends org.apache.catalina.session.StandardSession {

	private static String DIRTY_FLAG = "__changed__";
	private static final long serialVersionUID = -5219705900405324572L;
	protected volatile transient boolean dirty = false;
	private transient String site;
	private transient int checksum;

	public Session(Manager manager) {
		super(manager);
	}

	@Override
	public void setAttribute(String key, Object value) {
		super.setAttribute(key, value);
		markDirty();
	}

	@Override
	public void removeAttribute(String name) {
		super.removeAttribute(name);
		markDirty();
	}

	@Override
	public void setPrincipal(Principal principal) {
		super.setPrincipal(principal);
		markDirty();
	}

	@Override
	public void setMaxInactiveInterval(int interval) {
		super.setMaxInactiveInterval(interval);
		markDirty();
	}

	public String getSite() {
		return site;
	}

	private void markDirty() {
		dirty = true;
	}

	public boolean isDirty() {
		return dirty;
	}

	private void setClean() {
		removeAttribute(DIRTY_FLAG);
		this.dirty = false;
	}

	public synchronized int getChecksum() {
		return checksum;
	}

	public SessionData serialize() throws IOException {
		return serialize(null);
	}

	public synchronized int checksum() throws IOException {
		Map<String, Object> attributes = new HashMap<String, Object>();
		for (Enumeration<String> enumerator = getAttributeNames(); enumerator.hasMoreElements();) {
			String key = enumerator.nextElement();
			if (!DIRTY_FLAG.equals(key)) {
				attributes.put(key, getAttribute(key));
			}
		}

		try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(bos))) {
			oos.writeUnshared(attributes);
			oos.flush();
			bos.flush();
			checksum = Arrays.hashCode(bos.toByteArray());
			return checksum;
		}
	}

	public synchronized SessionData serialize(String alternativeSiteName) throws IOException {
		try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(bos)) {
			setClean();
			writeObjectData(oos);
			oos.flush();
			bos.flush();
			site = null == alternativeSiteName ? Utils.getSiteName(this) : alternativeSiteName;
			return new SessionData(getId(), site, bos.toByteArray(), getLastAccessedTimeInternal(),
					getMaxInactiveInterval(), checksum());
		}
	}

	public static Session load(Manager manager, SessionData sessionData) throws IOException, ClassNotFoundException {
		Session session = null;
		try (ByteArrayInputStream is = new ByteArrayInputStream(sessionData.getData());
				ObjectInputStream ois = Utils.getObjectInputStream(is, sessionData.getSite(), manager.getContext())) {
			Session loadedSession = (Session) manager.createEmptySession();
			loadedSession.readObjectData(ois);
			// isValid() calls manager.remove(session, true) in case the session expired
			if (loadedSession.isValid()) {
				session = loadedSession;
				session.site = sessionData.getSite();
				session.access();
				session.setClean();
			}
		}
		return session;
	}

}
