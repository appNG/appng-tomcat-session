package org.appng.tomcat.session.hazelcast;

import java.io.IOException;

import org.apache.catalina.Container;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.Session;
import org.apache.catalina.session.PersistentManagerBase;
import org.apache.juli.logging.Log;
import org.appng.tomcat.session.Utils;

public class HazelcastPersistentManager extends PersistentManagerBase {

	private final Log log = Utils.getLog(HazelcastSessionTrackerValve.class);

	private String name;

	protected void destroyInternal() throws LifecycleException {
		super.destroyInternal();
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
		// do not call super, instead load the session directly from the store
		try {
			log.info("Manager FIND: " + id);
			return getStore().load(id);
		} catch (ClassNotFoundException e) {
			throw new IOException("error loading class for session " + id, e);
		}
	}

	@Override
	protected String generateSessionId() {
		String sessionId = super.generateSessionId();
		log.info("Manager CREATED: " + sessionId);
		return sessionId;
	}

	@Override
	public void removeSuper(Session session) {
		log.info("Manager REMOVE SUPER: " + session.getId());
		super.removeSuper(session);
	}

	@Override
	public String getName() {
		if (this.name == null) {
			Container container = getContext();

			String contextName = container.getName();
			if (!contextName.startsWith("/")) {
				contextName = "/" + contextName;
			}

			String hostName = "";
			String engineName = "";

			if (container.getParent() != null) {
				Container host = container.getParent();
				hostName = host.getName();
				if (host.getParent() != null) {
					engineName = host.getParent().getName();
				}
			}

			this.name = "/" + engineName + "/" + hostName + contextName;
		}
		return this.name;
	}

}
