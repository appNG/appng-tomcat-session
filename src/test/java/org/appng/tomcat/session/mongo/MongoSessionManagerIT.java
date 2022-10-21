/*
 * Copyright 2015-2022 the original author or authors.
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
package org.appng.tomcat.session.mongo;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
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
import org.apache.catalina.startup.Tomcat.FixContextListener;
import org.apache.catalina.util.StandardSessionIdGenerator;
import org.appng.tomcat.session.Session;
import org.appng.tomcat.session.SessionData;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.testcontainers.containers.MongoDBContainer;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;

/**
 * Integration test for {@link MongoSessionManager}
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MongoSessionManagerIT {

	static MongoSessionManager manager;
	static StandardContext context;
	static MongoDBContainer mongo;

	@Test
	public void testSessionExpired() throws Exception {
		manager.setSessionSaveIntervalSeconds(2);
		Session session = manager.createEmptySession();
		session.setCreationTime(System.currentTimeMillis());
		session.setMaxInactiveInterval(6);
		session.setId("4711");
		session.setNew(true);
		session.setValid(true);
		session.setAttribute("foo", "bar");
		Assert.assertTrue(manager.commit(session));
		Thread.sleep(3000);

		Assert.assertTrue(manager.commit(session));
		Thread.sleep(7000);

		SessionData serialized = session.serialize("appng");
		Session created = Session.load(manager, serialized);
		Assert.assertNull(created);
	}

	@Test
	public void test() throws Exception {
		Session session = createSession();
		int checkSum1 = session.checksum();
		Map<String, Object> map = modifySession(session);
		int checkSum2 = assertSessionChanged(session, checkSum1, false);
		SessionData original = session.serialize();
		int checksum3 = original.checksum();
		Assert.assertEquals(checkSum2, checksum3);
		// --- basic tests

		modifySession(session, map);

		long accessedBefore = session.getThisAccessedTimeInternal();
		Session loaded = manager.findSession(session.getId());
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
		Assert.assertNotNull(manager.getSession(session.getId()));
		Assert.assertTrue(manager.commit(session));
		Assert.assertNotNull(manager.getSession(session.getId()));

		manager.remove(session);
	}

	public void modifySession(Session session, Map<String, Object> map) throws IOException {
		int oldChecksum = session.checksum();
		map.put("foo", "test");
		SessionData modified = session.serialize();
		int checksum = modified.checksum();
		Assert.assertNotEquals(oldChecksum, checksum);
		Assert.assertTrue(manager.commit(session));
	}

	private Session createSession() {
		Session session = manager.createEmptySession();
		session.setId("4711");
		session.setNew(true);
		session.setValid(true);
		session.setCreationTime(System.currentTimeMillis());
		Assert.assertTrue(session.isNew());
		Assert.assertFalse(session.isDirty());
		return session;
	}

	private Map<String, Object> modifySession(Session session) {
		session.setAttribute("foo", "test");
		Map<String, Object> map = new HashMap<>();
		session.setAttribute("amap", map);
		session.setAttribute("metaData", new MetaData());
		return map;
	}

	private int assertSessionChanged(Session session, int oldCheckSum, boolean isCommitted) throws IOException {
		int checksum = session.checksum();
		Assert.assertNotEquals(oldCheckSum, checksum);
		Assert.assertTrue(session.isDirty());
		Assert.assertTrue(manager.commit(session));
		Assert.assertFalse(session.isNew());
		Assert.assertFalse(session.isDirty());
		Assert.assertEquals(isCommitted, manager.commit(session));
		return checksum;
	}

	@Test
	public void testNonSticky() throws Exception {
		manager.getPersistentSessions().remove(new BasicDBObject());
		manager.clearAll();
		manager.setSticky(false);
		Session session = createSession();
		int checkSum1 = session.checksum();
		Map<String, Object> map = modifySession(session);
		int checkSum2 = assertSessionChanged(session, checkSum1, true);
		SessionData original = session.serialize();
		int checksum3 = original.checksum();
		Assert.assertEquals(checkSum2, checksum3);
		// ---------- same like normal test
		manager.removeLocal(session);

		modifySession(session, map);

		long accessedBefore = session.getThisAccessedTimeInternal();
		Session loaded = manager.findSession(session.getId());

		Assert.assertNotEquals(session, loaded);
		long accessedAfter = loaded.getThisAccessedTimeInternal();
		Assert.assertNotEquals(accessedBefore, accessedAfter);

		Assert.assertEquals(1, manager.getActiveSessions());
		manager.removeLocal(session);
		Assert.assertEquals(0, manager.getActiveSessions());

		Assert.assertTrue(Site.calledClassloader);
		loaded = manager.findSession(session.getId());
		Assert.assertTrue(Site.calledClassloader);
		Assert.assertEquals(1, manager.getActiveSessions());
		Assert.assertNotEquals(session, loaded);

		Assert.assertFalse(loaded.isDirty());
		Assert.assertEquals("test", session.getAttribute("foo"));
		session.setAttribute("he", "ho");
		Assert.assertNotNull(manager.getSession(session.getId()));
		Assert.assertTrue(manager.commit(session));
		Assert.assertNotNull(manager.getSession(session.getId()));
		manager.removeLocal(session);
		Assert.assertNull(manager.getSession(session.getId()));

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

		Session session = null;
		final int numSessions = 500;
		final int halfSessions = numSessions/2;
		for (int i = 0; i < numSessions; i++) {
			Session s = manager.createSession(null);
			s.setMaxInactiveInterval((i % 2 == 0 ? 1 : 3600));
			s.setAttribute("foo", "test");
			s.setAttribute("metaData", new MetaData());
			if (0 == i) {
				session = s;
				Assert.assertTrue(session.isNew());
			}
			manager.commit(s);
		}

		DBCollection persistentSessions = manager.getPersistentSessions();
		Assert.assertEquals(numSessions, persistentSessions.count());
		Assert.assertEquals(numSessions, manager.getActiveSessions());
		manager.commit(session);
		Assert.assertEquals(numSessions, persistentSessions.count());
		Assert.assertEquals(numSessions, manager.getActiveSessions());
		Assert.assertFalse(session.isNew());

		TimeUnit.SECONDS.sleep(2);
		manager.processExpires();
		Assert.assertEquals(halfSessions, manager.getExpiredSessions());
		Assert.assertEquals(halfSessions, persistentSessions.count());

		Assert.assertNull(manager.findSession(session.getId()));

		int activeSessions = numSessions / 2;
		Assert.assertEquals(activeSessions, manager.getActiveSessions());
		Assert.assertEquals(numSessions, created.get());
		Assert.assertEquals(activeSessions, destroyed.get());

		for (org.apache.catalina.Session s : manager.findSessions()) {
			manager.commit((Session) s);
		}

		Assert.assertEquals(activeSessions, persistentSessions.count());

		try (ByteArrayOutputStream out = new ByteArrayOutputStream();
				ObjectOutputStream objectOutputStream = new ObjectOutputStream(out);) {
			objectOutputStream.writeObject("appNG");
			objectOutputStream.writeObject("clear it!");
//			manager.clearSessionsOnEvent = Arrays.asList("java.lang.String");
//			manager.getTopic().publish(out.toByteArray());
		}
		Thread.sleep(100);
		//Assert.assertEquals(0, manager.getActiveSessions());

	}

	@BeforeClass
	public static void setup() throws LifecycleException {

		mongo = new MongoDBContainer("mongo:3.4.9");
		mongo.start();
		String connectionString = mongo.getConnectionString();
		String hostAndPort = connectionString.substring(10);

		context = new StandardContext();

		manager = new MongoSessionManager();
		manager.setSessionIdGenerator(new StandardSessionIdGenerator());
		manager.setContext(context);
		manager.setConnectionUri(hostAndPort);

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
