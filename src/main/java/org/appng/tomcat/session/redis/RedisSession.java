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
package org.appng.tomcat.session.redis;

import java.io.IOException;
import java.security.Principal;
import java.util.Date;
import java.util.HashMap;

import org.apache.catalina.Manager;
import org.apache.catalina.session.StandardSession;
import org.apache.juli.logging.Log;
import org.appng.tomcat.session.Utils;

/**
 * A Redis-backed session
 */
public class RedisSession extends StandardSession {

	private final Log log = Utils.getLog(RedisSession.class);

	protected static Boolean manualDirtyTrackingSupportEnabled = true;

	public static void setManualDirtyTrackingSupportEnabled(Boolean enabled) {
		manualDirtyTrackingSupportEnabled = enabled;
	}

	protected static String manualDirtyTrackingAttributeKey = "__changed__";

	public static void setManualDirtyTrackingAttributeKey(String key) {
		manualDirtyTrackingAttributeKey = key;
	}

	protected HashMap<String, Object> changedAttributes;
	protected Boolean dirty;

	public RedisSession(Manager manager) {
		super(manager);
		resetDirtyTracking();
	}

	public Boolean isDirty() {
		return dirty || !changedAttributes.isEmpty();
	}

	public HashMap<String, Object> getChangedAttributes() {
		return changedAttributes;
	}

	public void resetDirtyTracking() {
		changedAttributes = new HashMap<>();
		dirty = false;
	}

	@Override
	public void setAttribute(String key, Object value) {
		if (manualDirtyTrackingSupportEnabled && manualDirtyTrackingAttributeKey.equals(key)) {
			dirty = true;
			return;
		}

		Object oldValue = getAttribute(key);
		super.setAttribute(key, value);

		if ((value != null || oldValue != null)
				&& (value == null && oldValue != null || oldValue == null && value != null
						|| !value.getClass().isInstance(oldValue) || !value.equals(oldValue))) {
			if (this.manager instanceof RedisSessionManager && ((RedisSessionManager) this.manager).getSaveOnChange()) {
				try {
					((RedisSessionManager) this.manager).save(this, true);
				} catch (IOException ex) {
					log.error("Error saving session on setAttribute (triggered by saveOnChange=true): "
							+ ex.getMessage());
				}
			} else {
				changedAttributes.put(key, value);
			}
		}
	}

	@Override
	public void removeAttribute(String name) {
		super.removeAttribute(name);
		if (this.manager instanceof RedisSessionManager && ((RedisSessionManager) this.manager).getSaveOnChange()) {
			try {
				((RedisSessionManager) this.manager).save(this, true);
			} catch (IOException ex) {
				log.error("Error saving session on setAttribute (triggered by saveOnChange=true): " + ex.getMessage());
			}
		} else {
			dirty = true;
		}
	}

	@Override
	public void setId(String id) {
		// Specifically do not call super(): it's implementation does unexpected things
		// like calling manager.remove(session.id) and manager.add(session).
		this.id = id;
	}

	@Override
	public void setPrincipal(Principal principal) {
		dirty = true;
		super.setPrincipal(principal);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("RedisSession[");
		sb.append(id);
		sb.append("] (created: ");
		sb.append(new Date(creationTime));
		sb.append(", accessed: ");
		sb.append(new Date(lastAccessedTime));
		sb.append(", expires: ");
		sb.append(new Date(lastAccessedTime + maxInactiveInterval * 1000));
		sb.append(")");
		return (sb.toString());
	}
}
