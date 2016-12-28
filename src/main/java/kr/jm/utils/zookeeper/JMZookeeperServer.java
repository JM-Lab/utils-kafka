package kr.jm.utils.zookeeper;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import kr.jm.utils.enums.OS;
import kr.jm.utils.exception.JMExceptionManager;
import kr.jm.utils.helper.JMLog;
import kr.jm.utils.helper.JMPath;

/**
 * The Class JMZookeeperServer.
 */
public class JMZookeeperServer extends ZooKeeperServer {
	private static final String ZOOKEEPER_DIR = "zookeeper-dir";

	private static final org.slf4j.Logger log =
			org.slf4j.LoggerFactory.getLogger(JMZookeeperServer.class);

	private String hostnameOrIp;
	private int port;
	private int numConnections = 1024;
	private ServerCnxnFactory serverFactory;

	/**
	 * Instantiates a new JM zookeeper server.
	 *
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public JMZookeeperServer() throws IOException {
		this(ZOOKEEPER_DIR);
	}

	/**
	 * Instantiates a new JM zookeeper server.
	 *
	 * @param zookeeperDirPath
	 *            the zookeeper dir path
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public JMZookeeperServer(String zookeeperDirPath) throws IOException {
		this(OS.getHostname(), OS.getAvailableLocalPort(),
				JMPath.getPath(zookeeperDirPath).toFile(), 2000);
	}

	/**
	 * Instantiates a new JM zookeeper server.
	 *
	 * @param hostnameOrIp
	 *            the hostname or ip
	 * @param port
	 *            the port
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public JMZookeeperServer(String hostnameOrIp, int port) throws IOException {
		this(hostnameOrIp, port, ZOOKEEPER_DIR);
	}

	/**
	 * Instantiates a new JM zookeeper server.
	 *
	 * @param hostnameOrIp
	 *            the hostname or ip
	 * @param port
	 *            the port
	 * @param zookeeperDirPath
	 *            the zookeeper dir path
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public JMZookeeperServer(String hostnameOrIp, int port,
			String zookeeperDirPath) throws IOException {
		this(hostnameOrIp, port, JMPath.getPath(zookeeperDirPath).toFile(),
				2000);
	}

	/**
	 * Instantiates a new JM zookeeper server.
	 *
	 * @param hostnameOrIp
	 *            the hostname or ip
	 * @param port
	 *            the port
	 * @param zookeeperDirPath
	 *            the zookeeper dir path
	 * @param tickTime
	 *            the tick time
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public JMZookeeperServer(String hostnameOrIp, int port,
			String zookeeperDirPath, int tickTime) throws IOException {
		this(hostnameOrIp, port, JMPath.getPath(zookeeperDirPath).toFile(),
				tickTime);
	}

	/**
	 * Instantiates a new JM zookeeper server.
	 *
	 * @param hostnameOrIp
	 *            the hostname or ip
	 * @param port
	 *            the port
	 * @param zookeeperDir
	 *            the zookeeper dir
	 * @param tickTime
	 *            the tick time
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public JMZookeeperServer(String hostnameOrIp, int port, File zookeeperDir,
			int tickTime) throws IOException {
		this(hostnameOrIp, port, zookeeperDir, zookeeperDir, tickTime);
	}

	/**
	 * Instantiates a new JM zookeeper server.
	 *
	 * @param hostnameOrIp
	 *            the hostname or ip
	 * @param port
	 *            the port
	 * @param snapDir
	 *            the snap dir
	 * @param logDir
	 *            the log dir
	 * @param tickTime
	 *            the tick time
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public JMZookeeperServer(String hostnameOrIp, int port, File snapDir,
			File logDir, int tickTime) throws IOException {
		super(snapDir, logDir, tickTime);
		this.hostnameOrIp = hostnameOrIp;
		this.port = port;
	}

	/**
	 * Start.
	 *
	 * @return the JM zookeeper server
	 */
	public JMZookeeperServer start() {
		JMLog.info(log, "start", hostnameOrIp, port, numConnections);
		try {
			serverFactory = ServerCnxnFactory.createFactory(
					new InetSocketAddress(hostnameOrIp, port), numConnections);
			serverFactory.startup(this);
		} catch (Exception e) {
			JMExceptionManager.logException(log, e, "start", hostnameOrIp, port,
					numConnections);
		}
		return this;
	}

	public Iterable<ServerCnxn> getConnection() {
		return serverFactory.getConnections();
	}

	/**
	 * Stop.
	 */
	public void stop() {
		serverFactory.closeAll();
		serverFactory.shutdown();
		shutdown();
	}

	public int getNumConnections() {
		return numConnections;
	}

	public void setNumConnections(int numConnections) {
		this.numConnections = numConnections;
	}
}
