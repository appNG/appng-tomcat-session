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
package org.appng.tomcat.session.hazelcast;

import java.security.Principal;

import org.apache.catalina.Manager;
import org.apache.catalina.Session;

/**
 * A {@link Session} that can be flagged as dirty.
 */
public class HazelCastSession extends org.apache.catalina.session.StandardSession {

	private static String DIRTY_FLAG = "__changed__";
	private static final long serialVersionUID = -5219705900405324572L;
	protected transient boolean dirty = false;

	public HazelCastSession(Manager manager) {
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
		getSession().removeAttribute(DIRTY_FLAG);
		this.dirty = false;
	}

}
