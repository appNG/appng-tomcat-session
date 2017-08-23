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
package org.appng.tomcat.session;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.lang.reflect.Constructor;
import java.util.Map;

import javax.servlet.ServletContext;

import org.apache.catalina.connector.Request;

public class Utils {

	public static boolean isTemplateRequest(Request request) {
		return request.getServletPath().startsWith(getTemplatePrefix(request.getServletContext()));
	}

	public static String getTemplatePrefix(ServletContext servletContext) {
		try {
			@SuppressWarnings("unchecked")
			Object platformConfig = ((Map<String, Object>) servletContext.getAttribute("PLATFORM"))
					.get("platformConfig");
			return (String) platformConfig.getClass().getMethod("getString", String.class).invoke(platformConfig,
					"templatePrefix");
		} catch (ReflectiveOperationException e) {
			throw new IllegalArgumentException(e);
		}
	}

	public static ObjectInputStream getObjectInputStream(ClassLoader classLoader, ServletContext ctx, byte[] data) {
		return getObjectInputStream(classLoader, ctx, new ByteArrayInputStream(data));
	}

	public static ObjectInputStream getObjectInputStream(ClassLoader classLoader, ServletContext ctx,
			InputStream data) {
		ObjectInputStream ois = null;
		ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
		try {
			Thread.currentThread().setContextClassLoader(classLoader);
			@SuppressWarnings("unchecked")
			Constructor<ObjectInputStream> constructor = (Constructor<ObjectInputStream>) classLoader
					.loadClass(Constants.INPUT_STREAM_CLASS)
					.getDeclaredConstructor(InputStream.class, ServletContext.class);

			ois = constructor.newInstance(data, ctx);
		} catch (ReflectiveOperationException e) {
			throw new IllegalArgumentException(e);
		} finally {
			Thread.currentThread().setContextClassLoader(contextClassLoader);
		}
		return ois;
	}

}
