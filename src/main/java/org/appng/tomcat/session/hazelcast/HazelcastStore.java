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
package org.appng.tomcat.session.hazelcast;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.TimeUnit;

import org.apache.catalina.Session;
import org.apache.catalina.Store;
import org.apache.catalina.session.StandardSession;
import org.apache.catalina.session.StoreBase;
import org.apache.juli.logging.Log;
import org.appng.tomcat.session.SessionData;
import org.appng.tomcat.session.Utils;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.ManagementCenterConfig;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryEvictedListener;

/**
 * A {@link Store} using Hazelcast's {@link IMap} to store sessions.
 * <p/>
 * Configuration parameters (defaults in brackets):
 * <ul>
 * <li>mapName (tomcat.sessions)<br/>
 * The name of the map used to store the sessions.</li>
 * <li>mode (multicast)<br/>
 * The mode to use, one of multicast, tcp, client, standalone.</li>
 * <li>group (dev)<br/>
 * The Hazelcast group to use.</li>
 * <li>addresses (localhost:5701)<br/>
 * A comma separated list of server addresses to use, only applies to 'client' mode.</li>
 * <li>port (5701)<br/>
 * The Hazelcast port to use.</li>
 * <li>multicastGroup (224.2.2.3)<br/>
 * The multicast group, only applies to 'multicast' mode.</li>
 * <li>multicastPort (54327)<br/>
 * The multicast port, only applies to 'multicast' mode.</li>
 * <li>multicastTimeoutSeconds (2)<br/>
 * The multicast timeout, only applies to 'multicast' mode.</li>
 * <li>multicastTimeToLive (32)<br/>
 * The multicast ttl, only applies to 'multicast' mode.</li>
 * <li>tcpMembers (<i>null</i>)<br/>
 * A comma separated list of members to connect to, only applies to 'tcp' mode.</li>
 * <li>clusterName (appNG)<br/>
 * The Hazelcast cluster name.</li>
 * <li>instanceName (dev)<br/>
 * The Hazelcast instance name.</li>
 * </ul>
 * 
 * @author Matthias Müller
 */
public class HazelcastStore extends StoreBase implements EntryEvictedListener<String, SessionData> {

	private final Log log = Utils.getLog(HazelcastStore.class);
	private HazelcastInstance instance;

	private String mapName = "tomcat.sessions";
	private Mode mode = Mode.MULTICAST;
	private String addresses = "localhost:5701";
	private int port = NetworkConfig.DEFAULT_PORT;

	private String multicastGroup = MulticastConfig.DEFAULT_MULTICAST_GROUP;
	private int multicastPort = MulticastConfig.DEFAULT_MULTICAST_PORT;
	private int multicastTimeoutSeconds = MulticastConfig.DEFAULT_MULTICAST_TIMEOUT_SECONDS;
	private int multicastTimeToLive = MulticastConfig.DEFAULT_MULTICAST_TTL;

	private String tcpMembers;
	private boolean managementCenterEnabled;
	private String clusterName = "appNG";
	private String instanceName = "dev";
	private String configFile = "hazelcast.xml";

	public enum Mode {
		MULTICAST, TCP, CLIENT, STANDALONE, CLASSPATH;
	}

	@Override
	protected void initInternal() {
		super.initInternal();
		Config config = getConfig();
		switch (mode) {
		case CLIENT:
			instance = HazelcastClient.getHazelcastClientByName(instanceName);
			if (null == instance) {
				ClientConfig clientConfig = new ClientConfig();
				clientConfig.setInstanceName(instanceName);
				String[] addressArr = addresses.split(",");
				for (String address : addressArr) {
					clientConfig.getNetworkConfig().addAddress(address.trim());
				}
				instance = HazelcastClient.newHazelcastClient(clientConfig);
				log.info(String.format("Using new client %s, connecting to %s", instanceName,
						clientConfig.getNetworkConfig().getAddresses()));
			} else {
				log.info(String.format("Using existing client %s", instanceName));
			}
			return;

		case TCP:
			TcpIpConfig tcpIpConfig = config.getNetworkConfig().getJoin().getTcpIpConfig();
			tcpIpConfig.setEnabled(true);
			if (null != tcpMembers) {
				tcpIpConfig.addMember(tcpMembers);
				log.info("Using TCP mode with members: " + tcpMembers);
			} else {
				log.warn("TCP mode is used, but tcpMembers is not set!");
			}
			instance = Hazelcast.getOrCreateHazelcastInstance(config);
			break;

		case MULTICAST:
			MulticastConfig multicastConfig = config.getNetworkConfig().getJoin().getMulticastConfig();
			multicastConfig.setEnabled(true);
			multicastConfig.setMulticastGroup(multicastGroup);
			multicastConfig.setMulticastPort(multicastPort);
			multicastConfig.setMulticastTimeoutSeconds(multicastTimeoutSeconds);
			multicastConfig.setMulticastTimeToLive(multicastTimeToLive);
			log.info("Using MULTICAST on " + multicastGroup + ":" + multicastPort);
			instance = Hazelcast.getOrCreateHazelcastInstance(config);
			break;
		case CLASSPATH:
			config = new ClasspathXmlConfig(configFile);
			instance = Hazelcast.getOrCreateHazelcastInstance(config);
			log.info("Using classpath config:" + getClass().getClassLoader().getResource(configFile));
			break;
		default:
			instance = Hazelcast.getOrCreateHazelcastInstance(config);
			log.info("Using STANDALONE config");
			break;
		}
		log.info(String.format("Using instance %s", instance));
		getSessions().addEntryListener(this, true);
	}

	public void entryEvicted(EntryEvent<String, SessionData> event) {
		SessionData oldValue = event.getOldValue();
		String siteName = oldValue.getSite();

		try (ByteArrayInputStream in = new ByteArrayInputStream(oldValue.getData());
				ObjectInputStream ois = Utils.getObjectInputStream(in, siteName, getManager().getContext())) {
			StandardSession session = new StandardSession(getManager());
			session.readObjectData(ois);
			session.expire(true);
		} catch (IOException | ClassNotFoundException e) {
			log.error("Error expiring session " + event.getKey(), e);
		}
	}

	private Config getConfig() {
		Config config = new Config();
		config.setClusterName(clusterName);
		config.setInstanceName(instanceName);
		config.getNetworkConfig().setPort(port);
		if (managementCenterEnabled) {
			config.setManagementCenterConfig(new ManagementCenterConfig());
		}
		config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
		config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
		return config;
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
			String site = Utils.getSiteName(session.getSession());
			SessionData sessionData = new SessionData(session.getId(), site, bos.toByteArray());
			getSessions().put(session.getId(), sessionData, session.getMaxInactiveInterval(), TimeUnit.SECONDS);
			log.debug("saved: " + sessionData);
		}
	}

	@Override
	public HazelcastPersistentManager getManager() {
		return (HazelcastPersistentManager) super.getManager();
	}

	public StandardSession load(String id) throws ClassNotFoundException, IOException {
		StandardSession session = null;
		try {
			// pessimistic lock block to prevent concurrency problems whilst finding sessions
			getSessions().lock(id);
			SessionData sessionData = getSessions().get(id);
			if (null == sessionData) {
				log.debug(String.format("Session %s not found in map %s", id, getMapNameInternal()));
			} else {
				try (ByteArrayInputStream is = new ByteArrayInputStream(sessionData.getData());
						ObjectInputStream ois = Utils.getObjectInputStream(is, sessionData.getSite(),
								manager.getContext())) {
					session = (StandardSession) this.manager.createEmptySession();
					session.readObjectData(ois);
					log.debug("loaded: " + sessionData);
				}
			}
		} finally {
			getSessions().unlock(id);
		}
		return session;
	}

	public void remove(String id) throws IOException {
		SessionData removed = getSessions().remove(id);
		log.debug("removed: " + removed);
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

	private IMap<String, SessionData> getSessions() {
		return instance.getMap(getMapNameInternal());
	}

	private String getMapNameInternal() {
		return getManager().getName() + mapName;
	}

	// getters and setters
	public void setMapName(String mapName) {
		this.mapName = mapName;
	}

	public void setMode(String mode) {
		if (null != mode) {
			this.mode = Mode.valueOf(mode.trim().toUpperCase());
		}
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
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

	public void setManagementCenterEnabled(boolean managementCenterEnabled) {
		this.managementCenterEnabled = managementCenterEnabled;
	}

	public String getConfigFile() {
		return configFile;
	}

	public void setConfigFile(String configFile) {
		this.configFile = configFile;
	}

}
