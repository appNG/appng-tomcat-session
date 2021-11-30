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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;

import org.apache.catalina.core.StandardContext;
import org.apache.catalina.core.StandardEngine;
import org.apache.catalina.core.StandardHost;
import org.apache.catalina.core.StandardService;
import org.apache.catalina.loader.WebappLoader;
import org.apache.catalina.session.StandardSession;
import org.appng.tomcat.session.hazelcast.HazelcastStore.Mode;
import org.junit.Assert;
import org.junit.Test;

public class HazelStoreIT {

	@Test
	public void testExpire() throws Exception {
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

		final AtomicBoolean created = new AtomicBoolean(false);
		final AtomicBoolean destroyed = new AtomicBoolean(false);
		context.addApplicationLifecycleListener(new HttpSessionListener() {

			public void sessionDestroyed(HttpSessionEvent se) {
				destroyed.set(true);
			}

			public void sessionCreated(HttpSessionEvent se) {
				created.set(true);
			}
		});
		loader.setContext(context);

		HazelcastPersistentManager manager = new HazelcastPersistentManager();
		manager.setContext(context);
		manager.init();

		HazelcastStore store = new HazelcastStore();
		store.setMode(Mode.STANDALONE.name());
		store.setManager(manager);
		store.start();
		manager.setStore(store);

		StandardSession session = manager.createSession("4711");
		session.setMaxInactiveInterval(3/* seconds */);
		session.setAttribute("foo", "test");

		Assert.assertTrue(session.isNew());
		store.save(session);
		Assert.assertTrue(session.isNew());

		TimeUnit.SECONDS.sleep(10);

		Assert.assertNull(store.load(session.getId()));
		Assert.assertTrue(created.get());
		Assert.assertTrue(destroyed.get());
	}

	@Test
	public void test() throws Exception {
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

		HazelcastPersistentManager manager = new HazelcastPersistentManager();
		manager.setContext(context);
		manager.init();

		HazelcastStore store = new HazelcastStore();
		store.setMode(Mode.STANDALONE.name());
		store.setManager(manager);
		store.start();
		manager.setStore(store);

		StandardSession session = new StandardSession(manager);
		session.setId("4711");
		session.setNew(true);
		session.setValid(true);
		session.setCreationTime(System.currentTimeMillis());

		session.setAttribute("foo", "test");

		store.save(session);

		session = store.load(session.getId());
		Assert.assertEquals("test", session.getAttribute("foo"));

		session.setAttribute("he", "ho");
		store.save(session);

		Assert.assertEquals(1, store.getSize());
		Assert.assertArrayEquals(new String[] { "4711" }, store.keys());

		store.remove(session.getId());
	}

}
