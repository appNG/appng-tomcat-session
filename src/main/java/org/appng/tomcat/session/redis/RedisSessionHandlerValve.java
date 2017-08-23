/*
 * Copyright 2015-2017 the original author or authors.
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

import javax.servlet.ServletException;

import org.apache.catalina.Session;
import org.apache.catalina.Valve;
import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import org.apache.catalina.valves.ValveBase;
import org.appng.tomcat.session.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Valve} that uses {@link RedisSessionManager} to store a {@link Session}
 */
public class RedisSessionHandlerValve extends ValveBase {

	private final Logger log = LoggerFactory.getLogger(RedisSessionHandlerValve.class);
	private RedisSessionManager manager;

	public void setRedisSessionManager(RedisSessionManager manager) {
		this.manager = manager;
	}

	public void invoke(Request request, Response response) throws IOException, ServletException {
		try {
			getNext().invoke(request, response);
		} finally {
			long start = System.currentTimeMillis();
			if (!Utils.isTemplateRequest(request)) {
				manager.afterRequest();
			}
			long duration = System.currentTimeMillis() - start;
			if (log.isDebugEnabled() && duration > 0) {
				log.debug("handling session for {} took {}ms", request.getServletPath(), duration);
			}
		}
	}

}
