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

import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.catalina.core.StandardContext;
import org.apache.catalina.core.StandardEngine;
import org.apache.catalina.core.StandardHost;
import org.apache.catalina.core.StandardService;
import org.apache.catalina.loader.WebappLoader;
import org.apache.catalina.session.StandardSession;
import org.junit.Assert;
import org.junit.Test;
import org.testcontainers.containers.MongoDBContainer;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;

/**
 * Integration test for {@link MongoStore}
 */
public class MongoStoreIT {

	@Test
	public void test() throws Exception {

		try (MongoDBContainer mongo = new MongoDBContainer("mongo:3.4.9")) {
			mongo.start();
			String connectionString = mongo.getConnectionString();
			String hostAndPort = connectionString.substring(10);

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

			MongoPersistentManager manager = new MongoPersistentManager();
			manager.setContext(context);

			MongoStore store = new MongoStore() {
				@Override
				protected DBCursor getSessionsToExpire() {
					Date olderThan = new Date(System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1));
					BasicDBObject expireQuery = new BasicDBObject(lastModifiedProperty,
							new BasicDBObject("$lte", olderThan));
					DBCursor toExpire = this.collection.find(expireQuery);
					debug("Found %s sessions to expire with query: %s (older than %s)", toExpire.size(), expireQuery,
							olderThan);
					return toExpire;
				}
			};
			store.setManager(manager);
			store.setConnectionUri(hostAndPort);
			store.setDbName("mongo_session_test");
			store.setCollectionName("mongo_session_test");
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

			manager.processExpires();
			Assert.assertEquals(0, store.getSize());
		}
	}

}
