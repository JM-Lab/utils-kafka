package kr.jm.utils.zookeeper;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import kr.jm.utils.enums.OS;
import kr.jm.utils.helper.JMLog;
import kr.jm.utils.helper.JMPath;

public class JMZookeeperServer extends ZooKeeperServer {
	private static final org.slf4j.Logger log =
			org.slf4j.LoggerFactory.getLogger(JMZookeeperServer.class);

	private String hostnameOrIp;
	private int port;
	private int numConnections = 1024;
	private ServerCnxnFactory serverFactory;

	public JMZookeeperServer() throws IOException {
		this("zookeeper-dir");
	}

	public JMZookeeperServer(String zookeeperDirPath) throws IOException {
		this(OS.getHostname(), OS.getAvailableLocalPort(),
				JMPath.getPath(zookeeperDirPath).toFile(), 2000);
	}

	public JMZookeeperServer(String hostnameOrIp, int port,
			String zookeeperDirPath) throws IOException {
		this(hostnameOrIp, port, JMPath.getPath(zookeeperDirPath).toFile(),
				2000);
	}

	public JMZookeeperServer(String hostnameOrIp, int port,
			String zookeeperDirPath, int tickTime) throws IOException {
		this(hostnameOrIp, port, JMPath.getPath(zookeeperDirPath).toFile(),
				tickTime);
	}

	public JMZookeeperServer(String hostnameOrIp, int port, File zookeeperDir,
			int tickTime) throws IOException {
		this(hostnameOrIp, port, zookeeperDir, zookeeperDir, tickTime);
	}

	public JMZookeeperServer(String hostnameOrIp, int port, File snapDir,
			File logDir, int tickTime) throws IOException {
		super(snapDir, logDir, tickTime);
		this.hostnameOrIp = hostnameOrIp;
		this.port = port;
	}

	public void start() {
		JMLog.info(log, "start", hostnameOrIp, port, numConnections);
		try {
			serverFactory = ServerCnxnFactory.createFactory(
					new InetSocketAddress(hostnameOrIp, port), numConnections);
			serverFactory.startup(this);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Iterable<ServerCnxn> getConnection() {
		return serverFactory.getConnections();
	}

	public void stop() {
		serverFactory.shutdown();
	}

	public int getNumConnections() {
		return numConnections;
	}

	public void setNumConnections(int numConnections) {
		this.numConnections = numConnections;
	}
}
