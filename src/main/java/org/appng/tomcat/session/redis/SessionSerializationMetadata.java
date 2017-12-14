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

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;

import org.apache.catalina.Session;

/**
 * Metadata used when (de)serializing a {@link Session}
 */
public class SessionSerializationMetadata implements Serializable {

	private byte[] sessionAttributesHash;

	public SessionSerializationMetadata() {
		this.sessionAttributesHash = new byte[0];
	}

	public byte[] getSessionAttributesHash() {
		return sessionAttributesHash;
	}

	public void setSessionAttributesHash(byte[] sessionAttributesHash) {
		this.sessionAttributesHash = sessionAttributesHash;
	}

	public void copyFieldsFrom(SessionSerializationMetadata metadata) {
		this.setSessionAttributesHash(metadata.getSessionAttributesHash());
	}

	private void writeObject(java.io.ObjectOutputStream out) throws IOException {
		out.writeInt(sessionAttributesHash.length);
		out.write(this.sessionAttributesHash);
	}

	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
		int hashLength = in.readInt();
		byte[] sessionAttributesHash = new byte[hashLength];
		in.read(sessionAttributesHash, 0, hashLength);
		this.sessionAttributesHash = sessionAttributesHash;
	}

	private void readObjectNoData() throws ObjectStreamException {
		this.sessionAttributesHash = new byte[0];
	}

}
