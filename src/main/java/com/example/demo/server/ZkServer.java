package com.example.demo.server;

import com.example.demo.client.ZkClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import java.io.File;
import java.net.InetSocketAddress;

@Slf4j
public class ZkServer {

    public static final int DEFAULT_SERVER_PORT = 2181;
    public static final int DEFAULT_SERVER_TIMEOUT = 5000;
    public static final int DEFAULT_SERVER_SESSION_TIMEOUT = 10000;


    private ZooKeeperServer zkServer;
    private ZkClient zkClient;
    private NIOServerCnxnFactory nioServerFactory;
    private int port;
    private int timeOut;
    private int sessionTimeOut;

    private String dataFilePath;
    private String logFilePath;

    public ZkServer(String dataFilePath, String logFilePath) {
        this(DEFAULT_SERVER_PORT, DEFAULT_SERVER_TIMEOUT, DEFAULT_SERVER_SESSION_TIMEOUT, dataFilePath, logFilePath);
    }

    public ZkServer(int port, int timeOut, int sessionTimeOut, String dataFilePath, String logFilePath) {
        this.port = port;
        this.timeOut = timeOut;
        this.sessionTimeOut = sessionTimeOut;
        this.dataFilePath = dataFilePath;
        this.logFilePath = logFilePath;
    }

    public void start(String path){
        try {
            File dataPath = new File(dataFilePath);
            File logPath = new File(logFilePath);
            startSingleZkServer(port, timeOut, dataPath, logPath);
            zkClient = new ZkClient(path, 5000);

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private void startSingleZkServer(int port, final int timeOut, File dataPath, File logPath){
        try {

            zkServer = new ZooKeeperServer(dataPath, logPath, timeOut);
            zkServer.setMinSessionTimeout(sessionTimeOut);
            nioServerFactory = new NIOServerCnxnFactory();
            int maxClientConnections = 0;
            nioServerFactory.configure(new InetSocketAddress(port), maxClientConnections);
            nioServerFactory.startup(zkServer);
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public void close(){
        try {
            if (zkClient != null){
                zkClient.close();
            }
        } catch (Exception e){
            e.printStackTrace();
        }

        if (nioServerFactory != null){
            nioServerFactory.shutdown();
            try {
                nioServerFactory.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();
            }

            nioServerFactory = null;
        }

        if (zkServer != null){
            zkServer.shutdown();
        }
        log.debug("Close Zkclient");
    }

    public int getPort() {
        return port;
    }

    public ZkClient getZkClient(){
        return zkClient;
    }

}
