package org.appng.tomcat.session.hazelcast;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.catalina.Session;
import org.apache.catalina.session.StandardSession;
import org.apache.catalina.session.StoreBase;
import org.apache.commons.lang3.StringUtils;
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
	private int multicastTtl = MulticastConfig.DEFAULT_MULTICAST_TTL;

	private String tcpMembers = "localhost:5701";

	@Override
	protected void initInternal() {
		super.initInternal();
		Config config = new Config();
		config.getNetworkConfig().setPort(port);
		JoinConfig joinConfig = config.getNetworkConfig().getJoin();
		switch (mode) {
		case "client":
			ClientConfig clientConfig = new ClientConfig();
			clientConfig.getGroupConfig().setName(group);
			String[] addressArr = addresses.split(",");
			for (String address : addressArr) {
				clientConfig.getNetworkConfig().addAddress(address.trim());
			}
			instance = HazelcastClient.newHazelcastClient(clientConfig);
			break;

		case "tcp":
			joinConfig.getTcpIpConfig().setEnabled(true);
			joinConfig.getMulticastConfig().setEnabled(false);
			joinConfig.getTcpIpConfig().addMember(tcpMembers);
			break;

		default:
			joinConfig.getTcpIpConfig().setEnabled(false);
			joinConfig.getMulticastConfig().setEnabled(true);
			joinConfig.getMulticastConfig().setMulticastGroup(multicastGroup);
			joinConfig.getMulticastConfig().setMulticastPort(multicastPort);
			joinConfig.getMulticastConfig().setMulticastTimeoutSeconds(multicastTimeoutSeconds);
			joinConfig.getMulticastConfig().setMulticastTimeToLive(multicastTtl);
			break;
		}
		instance = Hazelcast.newHazelcastInstance(config);
	}

	public void save(Session session) throws IOException {
		try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(bos)) {

			((StandardSession) session).writeObjectData(oos);

			byte[] data = bos.toByteArray();

			HazelcastSession hzSession = new HazelcastSession();
			hzSession.setId(session.getIdInternal());
			hzSession.setApplication(getManager().getName());
			hzSession.setCreated(session.getCreationTime());
			hzSession.setData(data);
			hzSession.setLastModified(session.getLastAccessedTimeInternal());
			getMap().put(getManager().getName() + "_" + session.getId(), hzSession);
			log.info("saved: " + session.getId() + " (" + session + ")");
		}
	}

	@Override
	public HazelcastPersistentManager getManager() {
		return (HazelcastPersistentManager) super.getManager();
	}

	public Session load(String id) throws ClassNotFoundException, IOException {
		HazelcastSession hzSession = getMap().get(getManager().getName() + "_" + id);
		if (null == hzSession) {
			return null;
		}
		StandardSession session = null;

		byte[] data = (byte[]) hzSession.getData();
		if (data != null) {
			ClassLoader appContextLoader = getManager().getContext().getLoader().getClassLoader();
			try (ObjectInputStream ois = Utils.getObjectInputStream(appContextLoader,
					manager.getContext().getServletContext(), data)) {

				session = (StandardSession) this.manager.createEmptySession();
				session.readObjectData(ois);

			}
			log.info("loaded" + id + " (" + session + ")");
		}
		return session;
	}

	public void remove(String id) throws IOException {
		HazelcastSession removed = getMap().remove(id);
		log.info("removed" + id + " (" + removed + ")");
	}

	public String[] keys() throws IOException {
		return getMap().keySet().toArray(new String[0]);
	}

	public int getSize() throws IOException {
		return getMap().size();
	}

	public void clear() throws IOException {
		getMap().clear();
	}

	private ReplicatedMap<String, HazelcastSession> getMap() {
		return instance.getReplicatedMap(mapName);
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

	public void setMulticastTtl(int multicastTtl) {
		this.multicastTtl = multicastTtl;
	}

	public void setTcpMembers(String tcpMembers) {
		this.tcpMembers = tcpMembers;
	}

}
