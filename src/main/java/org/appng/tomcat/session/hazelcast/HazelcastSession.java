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
package org.appng.tomcat.session.hazelcast;

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
import org.apache.catalina.Session;
import org.appng.tomcat.session.SessionData;
import org.appng.tomcat.session.Utils;

/**
 * A {@link Session} that can be flagged as dirty.
 */
public class HazelcastSession extends org.apache.catalina.session.StandardSession {

	private static String DIRTY_FLAG = "__changed__";
	private static final long serialVersionUID = -5219705900405324572L;
	protected volatile transient boolean dirty = false;

	public HazelcastSession(Manager manager) {
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

	private void markDirty() {
		dirty = true;
	}

	public boolean isDirty() {
		return dirty;
	}

	public void setClean() {
		removeAttribute(DIRTY_FLAG);
		this.dirty = false;
	}

	public SessionData serialize() throws IOException {
		return serialize(null);
	}

	public int checksum() throws IOException {
		Map<String, Object> attributes = new HashMap<String, Object>();
		for (Enumeration<String> enumerator = getAttributeNames(); enumerator.hasMoreElements();) {
			String key = enumerator.nextElement();
			if (!DIRTY_FLAG.equals(key)) {
				attributes.put(key, getAttribute(key));
			}
		}

		try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(bos));) {
			oos.writeUnshared(attributes);
			oos.flush();
			bos.flush();
			int checksum = Arrays.hashCode(bos.toByteArray());
			return checksum;
		}
	}

	public SessionData serialize(String alternativeSiteName) throws IOException {
		try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(bos)) {
			setClean();
			writeObjectData(oos);
			oos.flush();
			bos.flush();
			String siteName = null == alternativeSiteName ? Utils.getSiteName(this) : alternativeSiteName;
			return new SessionData(getId(), siteName, bos.toByteArray(), checksum());
		}
	}

	public static HazelcastSession create(Manager manager, SessionData sessionData)
			throws IOException, ClassNotFoundException {
		try (ByteArrayInputStream is = new ByteArrayInputStream(sessionData.getData());
				ObjectInputStream ois = Utils.getObjectInputStream(is, sessionData.getSite(), manager.getContext())) {
			HazelcastSession session = (HazelcastSession) manager.createEmptySession();
			session.readObjectData(ois);
			session.access();
			session.setClean();
			manager.add(session);
			return session;
		}
	}

}
