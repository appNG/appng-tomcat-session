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
package org.appng.tomcat.session;

import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.catalina.core.StandardContext;
import org.apache.catalina.session.StandardManager;
import org.apache.catalina.session.StandardSession;
import org.apache.juli.logging.Log;
import org.appng.api.model.Property;
import org.appng.api.model.SimpleProperty;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.mock.web.MockServletContext;

public class UtilsTest {

	Log log = Utils.getLog(UtilsTest.class);

	@Test
	public void test() throws Exception {
		MockServletContext ctx = new MockServletContext();
		StandardManager manager = new StandardManager();
		manager.setContext(new StandardContext());
		StandardSession session = new StandardSession(manager);
		session.setId("4711");
		session.setValid(true);
		session.setAttribute("foo", "bar");
		session.setAttribute("property", new SimpleProperty("foo", "bar"));
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		session.writeObjectData(oos);
		oos.flush();
		oos.close();
		ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
		ObjectInputStream ois = Utils.getObjectInputStream(contextClassLoader, ctx, baos.toByteArray());

		StandardSession newSession = new StandardSession(manager);
		newSession.readObjectData(ois);
		Assert.assertEquals(session.getId(), newSession.getId());
		Assert.assertEquals("4711", newSession.getId());
		Assert.assertEquals(session.getAttribute("foo"), newSession.getAttribute("foo"));
		Assert.assertEquals("bar", newSession.getAttribute("foo"));
		Assert.assertEquals(SimpleProperty.class, newSession.getAttribute("property").getClass());
		Assert.assertEquals("bar", ((Property) newSession.getAttribute("property")).getString());

	}

	@Test
	public void testLogString() {
		log.info("testLogString");
	}

	@Test
	public void testLogObject() {
		log.info(new Object());
	}

	@Test
	public void testLogStringWithException() {
		log.info("testLogStringWithException", new IllegalArgumentException("BOOOM!"));
	}

	@Test
	public void testLogObjectWithException() {
		log.info(new Object(), new IllegalArgumentException("BOOOM!"));
	}

	@Test
	public void testLogNullWithException() {
		log.info(null, new IllegalArgumentException("BOOOM!"));
	}
	
	@Test
	public void testIsLevelEnabled(){
		Assert.assertTrue(log.isInfoEnabled());
		Assert.assertTrue(log.isWarnEnabled());
		Assert.assertTrue(log.isErrorEnabled());
	}

}
