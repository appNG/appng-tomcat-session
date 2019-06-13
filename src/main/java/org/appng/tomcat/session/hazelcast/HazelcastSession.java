package org.appng.tomcat.session.hazelcast;

import java.io.IOException;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

public class HazelcastSession implements DataSerializable {
	private String id;
	private String application;
	private byte[] data;
	private Long created;
	private Long lastModified;

	public void writeData(ObjectDataOutput out) throws IOException {
		out.writeUTF(id);
		out.writeByteArray(data);
		out.writeObject(created);
		out.writeObject(lastModified);
	}

	public void readData(ObjectDataInput in) throws IOException {
		id = in.readUTF();
		data = in.readByteArray();
		created = in.readObject();
		lastModified = in.readObject();
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getApplication() {
		return application;
	}

	public void setApplication(String application) {
		this.application = application;
	}

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}

	public Long getCreated() {
		return created;
	}

	public void setCreated(Long created) {
		this.created = created;
	}

	public Long getLastModified() {
		return lastModified;
	}

	public void setLastModified(Long lastModified) {
		this.lastModified = lastModified;
	}

}
