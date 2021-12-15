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
package org.appng.tomcat.session.hazelcast.v2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.core.StandardContext;
import org.apache.catalina.core.StandardEngine;
import org.apache.catalina.core.StandardHost;
import org.apache.catalina.core.StandardService;
import org.apache.catalina.loader.WebappLoader;
import org.apache.catalina.util.StandardSessionIdGenerator;
import org.appng.tomcat.session.hazelcast.HazelCastSession;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class HazelStoreIT {

	static HazelcastManager manager;

	@Test
	public void test() throws Exception {

		HazelCastSession session = manager.createEmptySession();
		session.setId("4711");
		session.setNew(true);
		session.setValid(true);
		session.setCreationTime(System.currentTimeMillis());

		Assert.assertFalse(session.isDirty());
		session.setAttribute("foo", "test");
		session.setAttribute("metaData", new MetaData());
		Assert.assertTrue(session.isDirty());
		manager.commit(session);
		Assert.assertFalse(session.isDirty());

		HazelCastSession loaded = manager.findSession(session.getId());
		Assert.assertEquals(session, loaded);

		Assert.assertEquals(1, manager.getActiveSessions());
		manager.removeLocal(session);
		Assert.assertEquals(0, manager.getActiveSessions());

		Assert.assertFalse(Site.calledClassloader);
		loaded = manager.findSession(session.getId());
		Assert.assertTrue(Site.calledClassloader);
		Assert.assertEquals(1, manager.getActiveSessions());
		Assert.assertNotEquals(session, loaded);

		Assert.assertFalse(loaded.isDirty());
		Assert.assertEquals("test", session.getAttribute("foo"));
		session.setAttribute("he", "ho");
		manager.commit(session);

		manager.remove(session);
	}

	@Test
	public void testExpire() throws Exception {

		final AtomicInteger created = new AtomicInteger(0);
		final AtomicInteger destroyed = new AtomicInteger(0);
		((StandardContext) manager.getContext()).addApplicationLifecycleListener(new HttpSessionListener() {

			public void sessionDestroyed(HttpSessionEvent se) {
				destroyed.getAndIncrement();
			}

			public void sessionCreated(HttpSessionEvent se) {
				created.getAndIncrement();
			}
		});

		HazelCastSession session = null;
		int numSessions = 10000;
		for (int i = 0; i < numSessions; i++) {
			HazelCastSession s = manager.createSession(null);
			s.setMaxInactiveInterval((i % 2 == 0 ? 1 : 3600));
			s.setAttribute("foo", "test");
			s.setAttribute("metaData", new MetaData());
			if (0 == i) {
				session = s;
			}
		}

		Assert.assertTrue(session.isNew());
		Assert.assertEquals(0, manager.getPersistentSize());
		Assert.assertEquals(numSessions, manager.getActiveSessions());
		manager.commit(session);
		Assert.assertEquals(1, manager.getPersistentSize());
		Assert.assertEquals(numSessions, manager.getActiveSessions());
		Assert.assertTrue(session.isNew());

		TimeUnit.SECONDS.sleep(2);
		manager.processExpires();
		Assert.assertEquals(0, manager.getPersistentSize());

		Assert.assertNull(manager.findSession(session.getId()));
		Assert.assertEquals(numSessions / 2, manager.getActiveSessions());
		Assert.assertEquals(numSessions, created.get());
		Assert.assertEquals(numSessions / 2, destroyed.get());
	}

	@BeforeClass
	public static void setup() throws LifecycleException {
		StandardContext context = new StandardContext();
		context.setName("foo");
		WebappLoader loader = new WebappLoader() {
			@Override
			public ClassLoader getClassLoader() {
				return WebappLoader.class.getClassLoader();
			}
		};
		context.setLoader(loader);
		StandardHost host = new StandardHost();
		StandardEngine engine = new StandardEngine();
		engine.setService(new StandardService());
		host.setParent(engine);
		context.setParent(host);
		loader.setContext(context);

		Map<String, Object> platform = new HashMap<String, Object>();
		Map<String, Site> sites = new HashMap<String, Site>();
		platform.put("sites", sites);
		sites.put("appNG", new Site());
		context.getServletContext().setAttribute("PLATFORM", platform);

		manager = new HazelcastManager();
		manager.setSessionIdGenerator(new StandardSessionIdGenerator());
		manager.setConfigFile("hazelcast-test.xml");
		manager.setContext(context);
		manager.init();
	}

	public static class Site {
		static boolean calledClassloader = false;

		public ClassLoader getSiteClassLoader() {
			calledClassloader = true;
			return getClass().getClassLoader();
		}
	}

	public static class MetaData implements Serializable {

		private static final long serialVersionUID = 1L;

		public String getSite() {
			return "appNG";
		}
	}

}
