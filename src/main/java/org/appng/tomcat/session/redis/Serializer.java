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
package org.appng.tomcat.session.redis;

import java.io.IOException;

/**
 * SPI to serialize/deserialize a {@link RedisSession}
 */
public interface Serializer {

	void setClassLoader(ClassLoader loader) throws ReflectiveOperationException;

	byte[] attributesHashFrom(RedisSession session) throws IOException;

	byte[] serializeFrom(RedisSession session, SessionSerializationMetadata metadata) throws IOException;

	void deserializeInto(byte[] data, RedisSession session, SessionSerializationMetadata metadata)
			throws IOException, ReflectiveOperationException;

}
