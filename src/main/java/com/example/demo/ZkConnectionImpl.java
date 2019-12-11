package com.example.demo;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ZkConnectionImpl implements ZkConnection {

    private static final int SESSION_TIMEOUT = 5000;

    private ZooKeeper zk;
    private Lock zkLock = new ReentrantLock();

    private String serverUr;
    private int sessiontTimeOut;

    public ZkConnectionImpl(String serverUr) {
        this(serverUr, SESSION_TIMEOUT);
    }

    public ZkConnectionImpl(String serverUr, int sessiontTimeOut) {
        this.serverUr = serverUr;
        this.sessiontTimeOut = sessiontTimeOut;
    }

    @Override
    public void connect(Watcher watcher) {
        zkLock.lock();

        try {
            if (zk != null){
                throw new IllegalStateException("Zk Client started");
            }

            try {
                zk = new ZooKeeper(serverUr, sessiontTimeOut, watcher);
            }catch (Exception e){
                e.printStackTrace();
            }
        }finally {
            zkLock.unlock();
        }
    }

    @Override
    public void close() throws InterruptedException {
        zkLock.lock();

        try {
            if (zk != null){
                zk.close();
                zk = null;
            }
        } finally {
            zkLock.unlock();
        }
    }

    @Override
    public String create(String path, byte[] data, CreateMode mode) throws KeeperException, InterruptedException {
        return zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
    }

    @Override
    public String create(String path, byte[] data, List<ACL> acl, CreateMode mode) throws KeeperException, InterruptedException {
        return zk.create(path, data, acl, mode);
    }

    @Override
    public void delete(String path) throws InterruptedException, KeeperException {
        zk.delete(path, -1);
    }

    @Override
    public void delete(String path, int version) throws InterruptedException, KeeperException {
        zk.delete(path, version);
    }

    @Override
    public boolean exists(String path, boolean watch) throws KeeperException, InterruptedException {
        return zk.exists(path, watch) != null;
    }

    @Override
    public List<String> getChildren(String path, boolean watch) throws KeeperException, InterruptedException {
        return zk.getChildren(path, watch);
    }

    @Override
    public byte[] readData(String path, Stat stat, boolean watch) throws KeeperException, InterruptedException {
        return zk.getData(path, watch, stat);
    }

    @Override
    public void writeData(String path, byte[] data) throws KeeperException, InterruptedException {
        writeData(path, data, -1);
    }

    @Override
    public void writeData(String path, byte[] data, int expectedVersion) throws KeeperException, InterruptedException {
        zk.setData(path, data, -1);
    }

    @Override
    public Stat writeDataReturnStat(String path, byte[] data, int expectedVersion) throws KeeperException, InterruptedException {
        return zk.setData(path, data, expectedVersion);
    }

    @Override
    public ZooKeeper.States getZookeeperState() {
        return zk != null ? zk.getState() : null;
    }

    @Override
    public long getCreateTime(String path) throws KeeperException, InterruptedException {
        Stat stat =  zk.exists(path, false);
        if (stat != null){
            return stat.getCtime();
        }
        return -1;
    }

    @Override
    public String getServers() {
        return serverUr;
    }

    @Override
    public List<OpResult> multi(Iterable<Op> ops) throws KeeperException, InterruptedException {
        return zk.multi(ops);
    }

    @Override
    public void addAuthInfo(String scheme, byte[] auth) {
        zk.addAuthInfo(scheme, auth);
    }

    @Override
    public void setAcl(String path, List<ACL> acl, int version) throws KeeperException, InterruptedException {
        zk.setACL(path, acl, version);
    }

    @Override
    public Map.Entry<List<ACL>, Stat> getAcl(String path) throws KeeperException, InterruptedException {
        Stat stat = new Stat();
        List<ACL> acl = zk.getACL(path, stat);
        return new AbstractMap.SimpleEntry<>(acl, stat);
    }
}
