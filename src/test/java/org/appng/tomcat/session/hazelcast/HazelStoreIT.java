/*
 * Copyright 2015-2019 the original author or authors.
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

import org.apache.catalina.core.StandardContext;
import org.apache.catalina.core.StandardEngine;
import org.apache.catalina.core.StandardHost;
import org.apache.catalina.core.StandardService;
import org.apache.catalina.loader.WebappLoader;
import org.apache.catalina.session.StandardSession;
import org.junit.Assert;
import org.junit.Test;

public class HazelStoreIT {

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
		
		HazelcastStore store = new HazelcastStore();
		store.setManager(manager);		
		manager.setStore(store);
		store.start();

		StandardSession session = new StandardSession(manager);
		session.setId("4711");
		session.setNew(true);
		session.setValid(true);
		session.setCreationTime(System.currentTimeMillis());

		session.setAttribute("foo", "test");

		store.save(session);

		StandardSession loaded = store.load(session.getId());
		Assert.assertEquals(session.getAttribute("foo"), loaded.getAttribute("foo"));

		Assert.assertEquals(1, store.getSize());
		Assert.assertArrayEquals(new String[] { "4711" }, store.keys());
	}

}
