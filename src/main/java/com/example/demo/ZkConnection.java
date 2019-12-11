package com.example.demo;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.Map;

public interface ZkConnection {

    public void connect(Watcher watcher);

    void close() throws InterruptedException;

    public String create(String path, byte[] data, CreateMode mode) throws KeeperException, InterruptedException;

    public String create(String path, byte[] data, List<ACL> acl, CreateMode mode) throws KeeperException, InterruptedException;

    public void delete(String path) throws InterruptedException, KeeperException;

    public void delete(String path, int version) throws InterruptedException, KeeperException;

    boolean exists(final String path, final boolean watch) throws KeeperException, InterruptedException;

    List<String> getChildren(final String path, final boolean watch) throws KeeperException, InterruptedException;

    public byte[] readData(String path, Stat stat, boolean watch) throws KeeperException, InterruptedException;

    public void writeData(String path, byte[] data) throws KeeperException, InterruptedException;

    public void writeData(String path, byte[] data, int expectedVersion) throws KeeperException, InterruptedException;

    public Stat writeDataReturnStat(String path, byte[] data, int expectedVersion) throws KeeperException, InterruptedException;

    public ZooKeeper.States getZookeeperState();

    public long getCreateTime(String path) throws KeeperException, InterruptedException;

    public String getServers();

    public List<OpResult> multi(Iterable<Op> ops) throws KeeperException, InterruptedException;

    public void addAuthInfo(String scheme, byte[] auth);

    public void setAcl(final String path, List<ACL> acl, int version) throws KeeperException, InterruptedException;

    public Map.Entry<List<ACL>, Stat> getAcl(final String path) throws KeeperException, InterruptedException;
}
