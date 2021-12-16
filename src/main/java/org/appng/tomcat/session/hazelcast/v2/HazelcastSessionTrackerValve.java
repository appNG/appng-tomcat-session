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
package org.appng.tomcat.session.hazelcast.v2;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.servlet.ServletException;

import org.apache.catalina.Session;
import org.apache.catalina.Valve;
import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import org.apache.catalina.valves.ValveBase;
import org.apache.juli.logging.Log;
import org.appng.tomcat.session.Utils;

/**
 * A {@link Valve} that uses {@link HazelcastManager} to store a {@link Session}
 */
public class HazelcastSessionTrackerValve extends ValveBase {

	private final Log log = Utils.getLog(HazelcastSessionTrackerValve.class);

	protected Pattern filter = Pattern.compile("^(/template/.*)|((/health)(\\?.*)?)$");

	@Override
	public void invoke(Request request, Response response) throws IOException, ServletException {
		try {
			getNext().invoke(request, response);
		} finally {
			Session session = request.getSessionInternal(false);
			if (commitRequired(request.getDecodedRequestURI()) && null != session) {
				long start = System.currentTimeMillis();
				HazelcastManager manager = (HazelcastManager) request.getContext().getManager();
				boolean committed = manager.commit(session);
				if (log.isDebugEnabled()) {
					log.debug(String.format("handling session %s for %s took %dms (committed: %s)", session.getId(),
							request.getServletPath(), System.currentTimeMillis() - start, committed));
				}
			}
		}
	}

	protected boolean commitRequired(String uri) {
		return null == filter || !filter.matcher(uri).matches();
	}

	public String getFilter() {
		return null == filter ? null : filter.toString();
	}

	public void setFilter(String filter) {
		if (filter == null || filter.length() == 0) {
			this.filter = null;
		} else {
			try {
				this.filter = Pattern.compile(filter);
			} catch (PatternSyntaxException pse) {
				log.error("ivalid pattern", pse);
			}
		}
	}

}
