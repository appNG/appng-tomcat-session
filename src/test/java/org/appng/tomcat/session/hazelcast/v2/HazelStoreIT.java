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
import org.apache.catalina.Session;
import org.apache.catalina.core.StandardContext;
import org.apache.catalina.core.StandardEngine;
import org.apache.catalina.core.StandardHost;
import org.apache.catalina.core.StandardService;
import org.apache.catalina.loader.WebappLoader;
import org.apache.catalina.startup.Tomcat.FixContextListener;
import org.apache.catalina.util.StandardSessionIdGenerator;
import org.appng.tomcat.session.SessionData;
import org.appng.tomcat.session.hazelcast.HazelcastSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.hazelcast.map.IMap;

public class HazelStoreIT {

	static HazelcastManager manager;
	static StandardContext context;

	@Test
	public void test() throws Exception {

		HazelcastSession session = manager.createEmptySession();
		session.setId("4711");
		session.setNew(true);
		session.setValid(true);
		session.setCreationTime(System.currentTimeMillis());

		int checkSum1 = session.checksum();

		Assert.assertFalse(session.isDirty());
		session.setAttribute("foo", "test");
		HashMap<String, String> map = new HashMap<String, String>();
		session.setAttribute("amap", map);
		session.setAttribute("metaData", new MetaData());

		int checkSum2 = session.checksum();
		Assert.assertNotEquals(checkSum1, checkSum2);

		Assert.assertTrue(session.isDirty());
		Assert.assertTrue(manager.commit(session));
		Assert.assertFalse(session.isDirty());
		Assert.assertFalse(manager.commit(session));

		SessionData original = session.serialize();
		int checksum3 = original.checksum();

		Assert.assertEquals(checkSum2, checksum3);

		map.put("foo", "test");
		SessionData modified = session.serialize();
		int checksum4 = modified.checksum();
		Assert.assertNotEquals(checksum3, checksum4);
		Assert.assertTrue(manager.commit(session));

		long accessedBefore = session.getThisAccessedTimeInternal();
		HazelcastSession loaded = manager.findSession(session.getId());
		Assert.assertEquals(session, loaded);
		long accessedAfter = session.getThisAccessedTimeInternal();
		Assert.assertNotEquals(accessedBefore, accessedAfter);

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

		HazelcastSession session = null;
		int numSessions = 10000;
		for (int i = 0; i < numSessions; i++) {
			HazelcastSession s = manager.createSession(null);
			s.setMaxInactiveInterval((i % 2 == 0 ? 1 : 3600));
			s.setAttribute("foo", "test");
			s.setAttribute("metaData", new MetaData());
			if (0 == i) {
				session = s;
			}
		}

		IMap<String, SessionData> persistentSessions = manager.getPersistentSessions();
		Assert.assertTrue(session.isNew());
		Assert.assertEquals(0, persistentSessions.size());
		Assert.assertEquals(numSessions, manager.getActiveSessions());
		manager.commit(session);
		Assert.assertEquals(1, persistentSessions.size());
		Assert.assertEquals(numSessions, manager.getActiveSessions());
		Assert.assertTrue(session.isNew());

		TimeUnit.SECONDS.sleep(2);
		manager.processExpires();
		Assert.assertEquals(0, persistentSessions.size());

		Assert.assertNull(manager.findSession(session.getId()));

		int activeSessions = numSessions / 2;
		Assert.assertEquals(activeSessions, manager.getActiveSessions());
		Assert.assertEquals(numSessions, created.get());
		Assert.assertEquals(activeSessions, destroyed.get());

		for (Session s : manager.findSessions()) {
			manager.commit(s);
		}

		Assert.assertEquals(activeSessions, persistentSessions.size());
	}

	@BeforeClass
	public static void setup() throws LifecycleException {

		context = new StandardContext();

		manager = new HazelcastManager();
		manager.setSessionIdGenerator(new StandardSessionIdGenerator());
		manager.setConfigFile("hazelcast-test.xml");
		manager.setContext(context);
		context.setManager(manager);

		context.setName("foo");
		WebappLoader loader = new WebappLoader() {
			@Override
			public ClassLoader getClassLoader() {
				return WebappLoader.class.getClassLoader();
			}
		};
		context.setLoader(loader);
		context.setWorkDir(new java.io.File("target/tomcat").getAbsolutePath());
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

		context.setConfigured(true);
		context.addLifecycleListener(new FixContextListener());
		context.start();

	}

	@AfterClass
	public static void tearDown() throws LifecycleException {
		context.stop();
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
