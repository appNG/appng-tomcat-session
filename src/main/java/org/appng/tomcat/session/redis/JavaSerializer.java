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
package org.appng.tomcat.session.redis;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import org.apache.juli.logging.Log;
import org.appng.tomcat.session.Utils;

/**
 * The default {@link Serializer} implementation
 */
public class JavaSerializer implements Serializer {

	private final Log log = Utils.getLog(JavaSerializer.class);

	private ClassLoader loader;

	public void setClassLoader(ClassLoader loader)
			throws NoSuchMethodException, SecurityException, ClassNotFoundException {
		this.loader = loader;
	}

	public byte[] attributesHashFrom(RedisSession session) throws IOException {
		Map<String, Object> attributes = new HashMap<String, Object>();
		for (Enumeration<String> enumerator = session.getAttributeNames(); enumerator.hasMoreElements();) {
			String key = enumerator.nextElement();
			attributes.put(key, session.getAttribute(key));
		}

		byte[] serialized = null;

		try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(bos));) {
			oos.writeUnshared(attributes);
			oos.flush();
			bos.flush();
			serialized = bos.toByteArray();
		}

		MessageDigest digester = null;
		try {
			digester = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			log.error("Unable to get MessageDigest instance for MD5");
		}
		return digester.digest(serialized);
	}

	public byte[] serializeFrom(RedisSession session, SessionSerializationMetadata metadata) throws IOException {
		byte[] serialized = null;

		try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(bos));) {
			oos.writeObject(metadata);
			session.writeObjectData(oos);
			oos.flush();
			serialized = bos.toByteArray();
		}

		return serialized;
	}

	public void deserializeInto(byte[] data, RedisSession session, SessionSerializationMetadata metadata)
			throws IOException, ReflectiveOperationException {
		ObjectInputStream ois = Utils.getObjectInputStream(loader, session.getServletContext(), data);
		ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(loader);
		try {
			SessionSerializationMetadata serializedMetadata = (SessionSerializationMetadata) ois.readObject();
			metadata.copyFieldsFrom(serializedMetadata);
			session.readObjectData(ois);
		} finally {
			Thread.currentThread().setContextClassLoader(contextClassLoader);
		}
	}
}
