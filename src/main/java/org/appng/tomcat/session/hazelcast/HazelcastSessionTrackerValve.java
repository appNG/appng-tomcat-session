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

import java.io.IOException;

import javax.servlet.ServletException;

import org.apache.catalina.Session;
import org.apache.catalina.Valve;
import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import org.apache.catalina.valves.PersistentValve;
import org.apache.juli.logging.Log;
import org.appng.tomcat.session.Utils;

/**
 * A {@link Valve} that uses {@link HazelcastPersistentManager} to store a {@link Session}
 */
public class HazelcastSessionTrackerValve extends PersistentValve {

	private final Log log = Utils.getLog(HazelcastSessionTrackerValve.class);

	@Override
	public void invoke(Request request, Response response) throws IOException, ServletException {
		try {
			getNext().invoke(request, response);
		} finally {
			long start = System.currentTimeMillis();
			HazelcastPersistentManager manager = (HazelcastPersistentManager) request.getContext().getManager();
			Session session = request.getSessionInternal(false);
			if (session != null) {
				if (session.isValid()) {
					log.trace(String.format("Request with session completed, saving session %s", session.getId()));
					manager.getStore().save(session);
				} else {
					log.trace(String.format("HTTP Session has been invalidated, removing %s", session.getId()));
					manager.remove(session);
				}
			}

			long duration = System.currentTimeMillis() - start;
			if (log.isDebugEnabled() && duration > 0) {
				log.debug(String.format("handling session for %s took %sms", request.getServletPath(), duration));
			}
		}
	}

}
