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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpSession;

import org.apache.catalina.Container;
import org.apache.catalina.Context;
import org.apache.catalina.Manager;
import org.apache.catalina.Session;
import org.apache.catalina.connector.Request;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

public class Utils {

	private static Log internalLog = LogFactory.getLog(Utils.class);
	private static Class<?> slf4jLoggerFactory;

	static {
		try {
			slf4jLoggerFactory = Utils.class.getClassLoader().loadClass("org.slf4j.LoggerFactory");
		} catch (ClassNotFoundException e) {
			internalLog.info("org.slf4j.LoggerFactory not present");
		}
	}

	public static Log getLog(Class<?> clazz) {
		if (null != slf4jLoggerFactory) {
			try {
				Object slf4jLogger = slf4jLoggerFactory.getMethod("getLogger", Class.class).invoke(null, clazz);
				return getSlf4jWrapper(clazz, slf4jLogger);
			} catch (Exception e) {
				LogFactory.getLog(Utils.class).error("error while retrieving slf4j logger", e);
			}
		}
		return LogFactory.getLog(clazz);
	}

	public static boolean isTemplateRequest(Request request) {
		String templatePrefix = getTemplatePrefix(request.getServletContext());
		return null != templatePrefix && request.getServletPath().startsWith(templatePrefix);
	}

	public static String getTemplatePrefix(ServletContext servletContext) {
		try {
			Map<String, Object> platformProperties = getPlatform(servletContext);
			if (null != platformProperties) {
				Object platformConfig = platformProperties.get("platformConfig");
				return (String) platformConfig.getClass().getMethod("getString", String.class).invoke(platformConfig,
						"templatePrefix");
			}
		} catch (ReflectiveOperationException e) {
			throw new IllegalArgumentException(e);
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	public static Map<String, Object> getPlatform(ServletContext servletContext) {
		return (Map<String, Object>) servletContext.getAttribute("PLATFORM");
	}

	public static ObjectInputStream getObjectInputStream(ClassLoader classLoader, ServletContext ctx, byte[] data)
			throws IOException {
		return getObjectInputStream(classLoader, ctx, new ByteArrayInputStream(data));
	}

	public static ObjectInputStream getObjectInputStream(ClassLoader classLoader, ServletContext ctx, InputStream data)
			throws IOException {
		try {
			@SuppressWarnings("unchecked")
			Constructor<ObjectInputStream> constructor = (Constructor<ObjectInputStream>) classLoader
					.loadClass(Constants.INPUT_STREAM_CLASS)
					.getDeclaredConstructor(InputStream.class, ServletContext.class);
			return constructor.newInstance(data, ctx);
		} catch (ReflectiveOperationException e) {
			// ignore, webapp is not appNG
		}
		return new ObjectInputStream(data);
	}

	private static Log getSlf4jWrapper(Class<?> clazz, Object slf4jLogger) {
		return new Log() {

			@SuppressWarnings("unchecked")
			private <T> T callRealMethod(String name, Object... args) {
				try {
					Class<?>[] types = new Class<?>[args.length];
					List<Object> argsList = Arrays.asList(args);
					if (types.length > 0) {
						types[0] = String.class;
						if (null == args[0]) {
							argsList.set(0, "");
						} else if (!String.class.isAssignableFrom(args[0].getClass())) {
							argsList.set(0, args[0].toString());
						}
						for (int i = 1; i < types.length; i++) {
							types[i] = Throwable.class.isAssignableFrom(args[i].getClass()) ? Throwable.class
									: Object.class;
						}
					}
					return (T) slf4jLogger.getClass().getMethod(name, types).invoke(slf4jLogger, argsList.toArray());
				} catch (Exception e) {
					LogFactory.getLog(clazz).error("error while using slf4j", e);
				}
				return null;
			}

			public void warn(Object message, Throwable t) {
				callRealMethod("warn", message, t);
			}

			public void warn(Object message) {
				callRealMethod("warn", message);
			}

			public void trace(Object message, Throwable t) {
				callRealMethod("trace", message, t);
			}

			public void trace(Object message) {
				callRealMethod("trace", message);
			}

			public boolean isWarnEnabled() {
				return callRealMethod("isWarnEnabled");
			}

			public boolean isTraceEnabled() {
				return callRealMethod("isTraceEnabled");
			}

			public boolean isInfoEnabled() {
				return callRealMethod("isInfoEnabled");
			}

			public boolean isFatalEnabled() {
				return isErrorEnabled();
			}

			public boolean isErrorEnabled() {
				return callRealMethod("isErrorEnabled");
			}

			public boolean isDebugEnabled() {
				return callRealMethod("isDebugEnabled");
			}

			public void info(Object message, Throwable t) {
				callRealMethod("info", message, t);
			}

			public void info(Object message) {
				callRealMethod("info", message);
			}

			public void fatal(Object message, Throwable t) {
				error(message, t);
			}

			public void fatal(Object message) {
				error(message);
			}

			public void error(Object message, Throwable t) {
				callRealMethod("error", message, t);
			}

			public void error(Object message) {
				callRealMethod("error", message);
			}

			public void debug(Object message, Throwable t) {
				callRealMethod("debug", message, t);
			}

			public void debug(Object message) {
				callRealMethod("debug", message);
			}

		};
	}

	public static String getContextName(Context context) {
		String contextName = context.getName();
		if (!contextName.startsWith("/")) {
			contextName = "/" + contextName;
		}
		String hostName = "";
		String engineName = "";
		if (context.getParent() != null) {
			Container host = context.getParent();
			hostName = host.getName();
			if (host.getParent() != null) {
				engineName = host.getParent().getName();
			}
		}
		return "/" + engineName + "/" + hostName + contextName;
	}

	public static Session[] findSessions(Manager manager, String[] keys, Log log) {
		List<Session> sessions = new ArrayList<>();
		for (String key : keys) {
			try {
				Session session = manager.findSession(key);
				if (null != session) {
					sessions.add(session);
				}
			} catch (IOException e) {
				log.warn(String.format("Error loading session: %s", key));
			}
		}
		return sessions.toArray(new Session[0]);
	}

	public static String getSiteName(HttpSession session) {
		Object metaData = session.getAttribute("metaData");
		if (null != metaData) {
			try {
				return (String) metaData.getClass().getMethod("getSite").invoke(metaData);
			} catch (ReflectiveOperationException e) {
				// ignore
			}
		}
		return null;
	}

	@SuppressWarnings("rawtypes")
	private static ClassLoader getClassLoader(String siteName, Context context) {
		Map<String, Object> platform = getPlatform(context.getServletContext());
		if (null != platform && null != siteName) {
			Object siteMap = (Map) platform.get("sites");
			Object site;
			if (null != siteMap && (site = ((Map) siteMap).get(siteName)) != null) {
				try {
					return (ClassLoader) site.getClass().getMethod("getSiteClassLoader").invoke(site);
				} catch (ReflectiveOperationException e) {
					// ignore
				}
			} else if (internalLog.isDebugEnabled()) {
				internalLog.debug(String.format("Site '%s' not found in context!", siteName));
			}
		}
		return context.getLoader().getClassLoader();
	}

	public static ObjectInputStream getObjectInputStream(InputStream in, String siteName, Context context)
			throws IOException {
		final ClassLoader classLoader = getClassLoader(siteName, context);
		if (internalLog.isDebugEnabled()) {
			internalLog.debug(String.format("Using %s for site '%s'.", classLoader.getClass().getSimpleName(), siteName));
		}
		return new ObjectInputStream(in) {
			protected Class<?> resolveClass(java.io.ObjectStreamClass desc) throws IOException, ClassNotFoundException {
				return Class.forName(desc.getName(), false, classLoader);
			}
		};
	}

}
