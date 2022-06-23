public class HostName {

	public static void main(String[] args) throws java.net.UnknownHostException {
		System.err.println(java.net.InetAddress.getLocalHost().getHostName());
	}

}
