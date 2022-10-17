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

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * Used to persist the binary representation of {@link org.apache.catalina.Session}.
 */
public class SessionData implements Serializable {

	private final String id;
	private final String site;
	private final byte[] data;
	private final long lastAccessed;
	private final int maxInactiveInterval;
	private final int checksum;

	public SessionData(String id, String site, byte[] data, long lastAccessed, int maxInactiveInterval, int checksum) {
		this.id = id;
		this.site = site;
		this.data = data;
		this.lastAccessed = lastAccessed;
		this.maxInactiveInterval = maxInactiveInterval;
		this.checksum = checksum;
	}

	public String getSite() {
		return site;
	}

	public byte[] getData() {
		return data;
	}

	public String getId() {
		return id;
	}

	/**
	 * last accessed time in milliseconds
	 * @return milliseconds since last access
	 */
	public long getLastAccessed() {
		return lastAccessed;
	}

	public int getMaxInactiveInterval() {
		return maxInactiveInterval;
	}

	@Override
	public String toString() {
		return String.format("[%s] %s (%db, checksum: %d)", site, id, data.length, checksum);
	}

	public int checksum() {
		return checksum;
	}

	public long secondsSinceAccessed() {
		return (System.currentTimeMillis() - lastAccessed) / 1000;
	}

}
