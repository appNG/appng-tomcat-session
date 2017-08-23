package org.appng.tomcat.session;

import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.catalina.core.StandardContext;
import org.apache.catalina.session.StandardManager;
import org.apache.catalina.session.StandardSession;
import org.appng.api.model.Property;
import org.appng.api.model.SimpleProperty;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.mock.web.MockServletContext;

public class UtilsTest {

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
}
