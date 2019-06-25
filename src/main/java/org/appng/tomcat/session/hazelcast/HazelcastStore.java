/*
 * Copyright 2015-2019 the original author or authors.
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
package org.appng.tomcat.session.hazelcast;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;

import org.apache.catalina.Session;
import org.apache.catalina.Store;
import org.apache.catalina.session.StandardSession;
import org.apache.catalina.session.StoreBase;
import org.apache.juli.logging.Log;
import org.appng.tomcat.session.Utils;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ReplicatedMap;

/**
 * A {@link Store} using Hazelcast's {@link ReplicatedMap} to store sessions.
 * <p/>
 * Configuration parameters (defaults in brackets):
 * <ul>
 * <li>mapName (tomcat.sessions)<br/>
 * The name of the map used to store the sessions.</li>
 * 
 * <li>mode (multicast)<br/>
 * The mode to use, one of multicast, tcp, client.</li>
 * 
 * <li>group (dev)<br/>
 * The Hazelcast group to use.</li>
 * 
 * <li>addresses (localhost:5701)<br/>
 * A comma separated list of server addresses to use, only applies to 'client'
 * mode.</li>
 * 
 * <li>port (5701)<br/>
 * The Hazelcast port to use.</li>
 * 
 * <li>multicastGroup (224.2.2.3)<br/>
 * The multicast group, only applies to 'multicast' mode.</li>
 * 
 * <li>multicastPort (54327)<br/>
 * The multicast port, only applies to 'multicast' mode.</li>
 * 
 * <li>multicastTimeoutSeconds (2)<br/>
 * The multicast timeout, only applies to 'multicast' mode.</li>
 * 
 * <li>multicastTimeToLive (32)<br/>
 * The multicast ttl, only applies to 'multicast' mode.</li>
 * 
 * <li>tcpMembers (<i>null</i>)<br/>
 * A comma separated list of members to connect to, only applies to 'tcp'
 * mode.</li>
 * 
 * <li>instanceName (appNG)<br/>
 * The Hazelcast instance name.</li>
 * </ul>
 * 
 * @author Matthias Müller
 *
 */
public class HazelcastStore extends StoreBase {

	private final Log log = Utils.getLog(HazelcastStore.class);
	private HazelcastInstance instance;

	private String mapName = "tomcat.sessions";
	private String mode = "multicast";
	private String group = "dev";
	private String addresses = "localhost:5701";
	private int port = NetworkConfig.DEFAULT_PORT;

	private String multicastGroup = MulticastConfig.DEFAULT_MULTICAST_GROUP;
	private int multicastPort = MulticastConfig.DEFAULT_MULTICAST_PORT;
	private int multicastTimeoutSeconds = MulticastConfig.DEFAULT_MULTICAST_TIMEOUT_SECONDS;
	private int multicastTimeToLive = MulticastConfig.DEFAULT_MULTICAST_TTL;

	private String tcpMembers;
	private String instanceName = "appNG";

	@Override
	protected void initInternal() {
		super.initInternal();
		Config config = new Config();
		config.setInstanceName(instanceName);
		config.getNetworkConfig().setPort(port);
		JoinConfig joinConfig = config.getNetworkConfig().getJoin();
		switch (mode) {
		case "client":
			ClientConfig clientConfig = new ClientConfig();
			clientConfig.getGroupConfig().setName(group);
			clientConfig.setInstanceName(instanceName);
			String[] addressArr = addresses.split(",");
			for (String address : addressArr) {
				clientConfig.getNetworkConfig().addAddress(address.trim());
			}
			instance = HazelcastClient.getHazelcastClientByName(instanceName);
			if (null == instance) {
				instance = HazelcastClient.newHazelcastClient(clientConfig);
			}
			log.info(String.format("Using client, connecting to %s", clientConfig.getNetworkConfig().getAddresses()));
			return;

		case "tcp":
			joinConfig.getTcpIpConfig().setEnabled(true);
			joinConfig.getMulticastConfig().setEnabled(false);
			if (null != tcpMembers) {
				joinConfig.getTcpIpConfig().addMember(tcpMembers);
			}
			instance = Hazelcast.getOrCreateHazelcastInstance(config);
			break;

		default:
			joinConfig.getTcpIpConfig().setEnabled(false);
			joinConfig.getMulticastConfig().setEnabled(true);
			joinConfig.getMulticastConfig().setMulticastGroup(multicastGroup);
			joinConfig.getMulticastConfig().setMulticastPort(multicastPort);
			joinConfig.getMulticastConfig().setMulticastTimeoutSeconds(multicastTimeoutSeconds);
			joinConfig.getMulticastConfig().setMulticastTimeToLive(multicastTimeToLive);
			instance = Hazelcast.getOrCreateHazelcastInstance(config);
			break;
		}
		log.info(String.format("Using instance %s", instance));
	}
	
	@Override
	protected void destroyInternal() {
		log.info(String.format("Shutting down instance %s", instance));
		instance.shutdown();
	}

	public void save(Session session) throws IOException {
		try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(bos)) {
			((StandardSession) session).writeObjectData(oos);
			getSessions().put(session.getId(), bos.toByteArray());
			log.debug("saved: " + session.getId());
		}
	}

	@Override
	public HazelcastPersistentManager getManager() {
		return (HazelcastPersistentManager) super.getManager();
	}

	public StandardSession load(String id) throws ClassNotFoundException, IOException {
		byte[] data = getSessions().get(id);
		if (null == data) {
			return null;
		}

		ClassLoader appContextLoader = getManager().getContext().getLoader().getClassLoader();
		try (ObjectInputStream ois = Utils.getObjectInputStream(appContextLoader,
				manager.getContext().getServletContext(), data)) {
			StandardSession session = (StandardSession) this.manager.createEmptySession();
			session.readObjectData(ois);
			log.debug("loaded: " + id);
			return session;
		}
	}

	public void remove(String id) throws IOException {
		getSessions().remove(id);
		log.debug("removed" + id);
	}

	public String[] keys() throws IOException {
		return getSessions().keySet().toArray(new String[0]);
	}

	public int getSize() throws IOException {
		return getSessions().size();
	}

	public void clear() throws IOException {
		getSessions().clear();
	}

	private Map<String, byte[]> getSessions() {
		return instance.getReplicatedMap(getManager().getName() + mapName);
	}

	// getters and setters
	public void setMapName(String mapName) {
		this.mapName = mapName;
	}

	public void setMode(String mode) {
		this.mode = mode;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public void setAddresses(String addresses) {
		this.addresses = addresses;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void setMulticastGroup(String multicastGroup) {
		this.multicastGroup = multicastGroup;
	}

	public void setMulticastPort(int multicastPort) {
		this.multicastPort = multicastPort;
	}

	public void setMulticastTimeoutSeconds(int multicastTimeoutSeconds) {
		this.multicastTimeoutSeconds = multicastTimeoutSeconds;
	}

	public void setMulticastTimeToLive(int multicastTimeToLive) {
		this.multicastTimeToLive = multicastTimeToLive;
	}

	public void setTcpMembers(String tcpMembers) {
		this.tcpMembers = tcpMembers;
	}

	public void setInstanceName(String instanceName) {
		this.instanceName = instanceName;
	}

}
