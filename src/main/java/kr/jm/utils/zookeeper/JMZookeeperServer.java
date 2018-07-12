package kr.jm.utils.zookeeper;

import kr.jm.utils.enums.OS;
import kr.jm.utils.exception.JMExceptionManager;
import kr.jm.utils.helper.JMLog;
import kr.jm.utils.helper.JMPath;
import kr.jm.utils.helper.JMString;
import kr.jm.utils.helper.JMThread;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.File;
import java.lang.reflect.Method;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.zookeeper.server.ZooKeeperServer.DEFAULT_TICK_TIME;

/**
 * The type Jm zookeeper server.
 */
public class JMZookeeperServer extends ZooKeeperServerMain {
    /**
     * The constant DEFAULT_ZOOKEEPER_DIR.
     */
    public static final String DEFAULT_ZOOKEEPER_DIR = "zookeeper-dir";

    private static final org.slf4j.Logger log =
            org.slf4j.LoggerFactory.getLogger(JMZookeeperServer.class);

    private int port;
    private Properties properties;
    private ExecutorService zookeeperThreadService;

    /**
     * Instantiates a new Jm zookeeper server.
     */
    public JMZookeeperServer() {
        this(DEFAULT_ZOOKEEPER_DIR);
    }

    /**
     * Instantiates a new Jm zookeeper server.
     *
     * @param zookeeperDirPath the zookeeper dir path
     */
    public JMZookeeperServer(String zookeeperDirPath) {
        this(2181, zookeeperDirPath);
    }

    /**
     * Instantiates a new Jm zookeeper server.
     *
     * @param port the port
     */
    public JMZookeeperServer(int port) {
        this(port, DEFAULT_ZOOKEEPER_DIR);
    }

    /**
     * Instantiates a new Jm zookeeper server.
     *
     * @param port             the port
     * @param zookeeperDirPath the zookeeper dir path
     */
    public JMZookeeperServer(int port, String zookeeperDirPath) {
        this(port, zookeeperDirPath, DEFAULT_TICK_TIME);
    }

    /**
     * Instantiates a new Jm zookeeper server.
     *
     * @param port             the port
     * @param zookeeperDirPath the zookeeper dir path
     * @param tickTime         the tick time
     */
    public JMZookeeperServer(int port, String zookeeperDirPath, int tickTime) {
        this(port, JMPath.getPath(zookeeperDirPath).toFile(),
                tickTime);
    }

    /**
     * Instantiates a new Jm zookeeper server.
     *
     * @param port     the port
     * @param dataDir  the data dir
     * @param tickTime the tick time
     */
    public JMZookeeperServer(int port, File dataDir, int tickTime) {
        this.port = port;
        this.zookeeperThreadService = JMThread.newSingleThreadPool();
        this.properties = new Properties();
        properties.setProperty("tickTime", String.valueOf(tickTime));
        properties.setProperty("dataDir", dataDir.getAbsolutePath());
        properties.setProperty("clientPort", String.valueOf(port));
    }

    /**
     * Start jm zookeeper server.
     *
     * @return the jm zookeeper server
     */
    public JMZookeeperServer start() {
        JMLog.info(log, "start", port);
        JMThread.runAsync(() -> {
            try {
                Thread.currentThread()
                        .setName("JMZookeeperServer-" + OS.getHostname());
                QuorumPeerConfig quorumPeerConfig = new QuorumPeerConfig();
                quorumPeerConfig.parseProperties(properties);
                ServerConfig configuration = new ServerConfig();
                configuration.readFrom(quorumPeerConfig);
                runFromConfig(configuration);
            } catch (Exception e) {
                JMExceptionManager
                        .handleExceptionAndThrowRuntimeEx(log, e, "start",
                                port);
            }
        }, zookeeperThreadService);

        return this;
    }

    /**
     * Stop.
     */
    public void stop() {
        log.info("shutdown starting {} ms !!!", System.currentTimeMillis());
        try {
            Method shutdown =
                    ZooKeeperServerMain.class.getDeclaredMethod("shutdown");
            shutdown.setAccessible(true);
            shutdown.invoke(this);
            zookeeperThreadService.shutdown();
            zookeeperThreadService.awaitTermination(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            JMExceptionManager.logException(log, e, "stop",
                    zookeeperThreadService.shutdownNow());
        }
        log.info("shutdown completely Over {} ms !!!",
                System.currentTimeMillis());
    }

    /**
     * Gets zookeeper connect.
     *
     * @return the zookeeper connect
     */
    public String getZookeeperConnect() {
        return JMString.buildIpOrHostnamePortPair(OS.getIp(), port);
    }
}
