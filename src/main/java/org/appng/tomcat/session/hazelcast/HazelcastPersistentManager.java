/*
 * Copyright 2015-2019 the original author or authors.
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

import java.io.IOException;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.Session;
import org.apache.catalina.session.PersistentManagerBase;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;
import org.appng.tomcat.session.Utils;

public class HazelcastPersistentManager extends PersistentManagerBase {

	private static final Log log = LogFactory.getLog(HazelcastPersistentManager.class);
	private String name;

	protected void destroyInternal() throws LifecycleException {
		super.destroyInternal();
		getStore().destroy();
	}

	@Override
	public HazelcastStore getStore() {
		return (HazelcastStore) super.getStore();
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
	public Session createSession(String sessionId) {
		Session session = super.createSession(sessionId);
		try {
			getStore().save(session);
		} catch (IOException e) {
			log.warn(String.format("Error creating session: %s", session.getIdInternal()));
			session = null;
		}
		return session;
	}

	@Override
	public Session findSession(String id) throws IOException {
		// do not call super, instead load the session directly from the store
		try {
			return getStore().load(id);
		} catch (ClassNotFoundException e) {
			throw new IOException("error loading class for session " + id, e);
		}
	}

	@Override
	public String getName() {
		if (this.name == null) {
			this.name = Utils.getContextName(getContext()).replace('/', '_');
		}
		return this.name;
	}

}
