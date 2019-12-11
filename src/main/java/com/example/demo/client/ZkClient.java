package com.example.demo.client;

import com.example.ZkChildListener;
import com.example.demo.ZkConnection;
import com.example.demo.ZkConnectionImpl;
import com.example.demo.ZkDataListener;
import com.example.demo.ZkStateListener;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import javax.swing.plaf.TextUI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
/**
 * TODO: retry reconnect
 */
public class ZkClient implements Watcher {


    private ZkConnection zkConnection;
    //TODO:
    private long retryTimeOut;

    private String serverUrl;
    private int timeOut;
    private Lock zkLock = new ReentrantLock();

    private Map<String, Set<ZkChildListener>> childListener = new ConcurrentHashMap<>();
    private Map<String, Set<ZkDataListener>> dataListener = new ConcurrentHashMap<>();
    private Set<ZkStateListener> stateListeners = new CopyOnWriteArraySet<>();

    private boolean closeTrig;
    private volatile boolean closed;

    public ZkClient(String serverUrl) {
        this(serverUrl, Integer.MAX_VALUE);
    }

    public ZkClient(String serverUrl, int timeOut) {
        this(new ZkConnectionImpl(serverUrl), timeOut);
    }

//    public ZkClient(String serverUrl, int timeOut, int connectionTimeout) {
//        this(new ZkConnectionImpl(serverUrl, timeOut), connectionTimeout);
//    }
//
//    public ZkClient(ZkConnection connection) {
//        this(connection, Integer.MAX_VALUE);
//    }

//    public ZkClient(ZkConnection connection, int connectionTimeout) {
//        this(connection, connectionTimeout);
//    }

    public ZkClient(ZkConnection zkConnection, int connectTimeOut) {
        if (zkConnection == null) {
            throw new NullPointerException("ZooKeeper connection is null");
        }
        zkConnection = zkConnection;

        connect(connectTimeOut, this);
    }

    public void setCloseTrig(boolean closeTrig) {
        this.closeTrig = closeTrig;
    }

    public boolean isCloseTrig() {
        return closeTrig;
    }

    public void connect(int connectTimeOut, Watcher watcher) {
        boolean starting = false;
        zkLock.lock();
        try {
            setCloseTrig(false);
            //TODO:start Thread
            zkConnection.connect(watcher);

            log.debug("start connect zookeeper server");
            starting = true;
        } finally {
            zkLock.unlock();
            if (!starting) {
                close();
            }
        }

    }

    public void close() {
        if (closed) {
            return;
        }
        zkLock.lock();
        try {

            setCloseTrig(true);
            //结束线程
            zkConnection.close();
            closed = true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            zkLock.unlock();
        }
    }

    public void createPersistent(String path, boolean createParent) {
        createPsersistent(path, createParent, ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }

    public void createPsersistent(String path, Object data, List<ACL> acls) {
        create(path, data, acls, CreateMode.PERSISTENT);
    }

    public String createPersistentSequential(String path, Object data) {
        return create(path, data, CreateMode.PERSISTENT_SEQUENTIAL);
    }

    public String createPersistentSequential(String path, Object data, List<ACL> acl) {
        return create(path, data, acl, CreateMode.PERSISTENT_SEQUENTIAL);
    }

    public void createEphemeral(String path) {
        create(path, null, CreateMode.EPHEMERAL);
    }

    public String create(String path, Object data, CreateMode mode) {
        return create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
    }

    //root
    public String create(String path, Object data, List<ACL> acls, CreateMode mode) {
        if (path == null) {
            throw new NullPointerException("Path is Empty");
        }
        if (acls == null || acls.size() == 0) {
            throw new NullPointerException("ACL is empty");
        }
        //投机取巧
        final byte[] bytes = String.valueOf(data).getBytes();

        return retryUntilConnected(new Callable<String>() {

            @Override
            public String call() {
                try {
                    return zkConnection.create(path, bytes, acls, mode);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return null;
            }
        });

    }

    public <T> T retryUntilConnected(Callable<T> callable) {
        while (true) {
            if (closed) {
                throw new IllegalStateException("Zkclient closed");
            }
            try {
                return callable.call();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    public List<String> subscribeChildChanges(String path, ZkChildListener listener) {
        synchronized (childListener) {
            Set<ZkChildListener> listeners = childListener.get(path);
            if (listeners == null) {
                listeners = new CopyOnWriteArraySet<ZkChildListener>();
                childListener.put(path, listeners);
            }
            listeners.add(listener);
        }
        return watchForChilds(path);
    }

    public void watchForData(final String path) {
        retryUntilConnected(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                zkConnection.exists(path, true);
                return null;
            }
        });
    }

    public List<String> watchForChilds(final String path) {
        return retryUntilConnected(new Callable<List<String>>() {
            @Override
            public List<String> call() throws Exception {
                exists(path, true);
                try {
                    return getChildren(path, true);
                } catch (Exception e) {
                    // ignore, the "exists" watch will listen for the parent node to appear
                }
                return null;
            }
        });
    }

    public void unsubscribeChildChanges(String path, ZkChildListener listener) {
        synchronized (childListener) {
            final Set<ZkChildListener> listeners = childListener.get(path);
            if (listeners != null) {
                listeners.remove(listener);
            }
        }
    }

    public void subscribeDataChanges(String path, ZkDataListener listener) {
        Set<ZkDataListener> listeners;
        synchronized (dataListener) {
            listeners = dataListener.get(path);
            if (listeners == null) {
                listeners = new CopyOnWriteArraySet<ZkDataListener>();
                dataListener.put(path, listeners);
            }
            listeners.add(listener);
        }
        watchForData(path);
    }

    public void unsubscribeDataChanges(String path, ZkDataListener listener) {
        synchronized (dataListener) {
            final Set<ZkDataListener> listeners = dataListener.get(path);
            if (listeners != null) {
                listeners.remove(listener);
            }
            if (listeners == null || listeners.isEmpty()) {
                dataListener.remove(path);
            }
        }
    }

    public void subscribeStateChanges(final ZkStateListener listener) {
        synchronized (stateListeners) {
            stateListeners.add(listener);
        }
    }

    public void unsubscribeStateChanges(ZkStateListener listener) {
        synchronized (stateListeners) {
            stateListeners.remove(listener);
        }
    }

    public void unsubscribeAll() {
        synchronized (childListener) {
            childListener.clear();
        }
        synchronized (dataListener) {
            dataListener.clear();
        }
        synchronized (stateListeners) {
            stateListeners.clear();
        }
    }


    @Override
    public void process(WatchedEvent watchedEvent) {
        boolean stateChanged = watchedEvent.getPath() == null;
        boolean znodeChanged = watchedEvent.getPath() != null;
        boolean dataChanged = watchedEvent.getType() == Event.EventType.NodeDataChanged ||
                watchedEvent.getType() == Event.EventType.NodeDeleted ||
                watchedEvent.getType() == Event.EventType.NodeCreated ||
                watchedEvent.getType() == Event.EventType.NodeChildrenChanged;
        zkLock.lock();
        try {
            if (isCloseTrig()) {
                return;
            }
            if (stateChanged) {
                processStateChanged(watchedEvent);
            }
            if (dataChanged) {
                processDataChanged(watchedEvent);
            }

        } finally {

            zkLock.unlock();
        }
    }

    private void processStateChanged(WatchedEvent event) {
        if (isCloseTrig()) {
            return;
        }
        handleStateChangeEvent(event.getState());
        if (event.getState() == Event.KeeperState.Expired) {
            try {
//                handleNewSessionEvents();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void handleStateChangeEvent(Event.KeeperState state) {
        for (ZkStateListener stateListener : stateListeners) {
            //获取开启一个线程
            try {
                stateListener.stateChanged(state);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void processDataChanged(WatchedEvent event) {
        final String path = event.getPath();

        if (event.getType() == Event.EventType.NodeChildrenChanged ||
                event.getType() == Event.EventType.NodeCreated ||
                event.getType() == Event.EventType.NodeDeleted) {
            Set<ZkChildListener> childListeners = childListener.get(path);
            if (childListeners != null && !childListeners.isEmpty()) {
                handleChildChangedEvents(path, childListeners);
            }
        }

        if (event.getType() == Event.EventType.NodeDataChanged || event.getType() == Event.EventType.NodeDeleted || event.getType() == Event.EventType.NodeCreated) {
            Set<ZkDataListener> listeners = dataListener.get(path);
            if (listeners != null && !listeners.isEmpty()) {
                try {
                    handleDataChangedEvents(event.getPath(), listeners);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void handleDataChangedEvents(final String path, Set<ZkDataListener> listeners) throws Exception {
        for (final ZkDataListener listener : listeners) {
            // reinstall watch
            exists(path, true);
            try {
                Object data = readData(path, null, true);
                listener.handleDataChange(path, data);
            } catch (Exception e) {
                listener.handleDataDeleted(path);
            }
        }
    }

    private void handleChildChangedEvents(final String path, Set<ZkChildListener> childListeners) {
        try {
            // reinstall the watch
            for (final ZkChildListener listener : childListeners) {
                try {
                    // if the node doesn't exist we should listen for the root node to reappear
                    exists(path);
                    List<String> children = getChildren(path);
                    listener.handleChildChange(path, children);
                } catch (Exception e) {
                    listener.handleChildChange(path, null);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    protected boolean exists(final String path, final boolean watch) {
        return retryUntilConnected(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return zkConnection.exists(path, watch);
            }
        });
    }

    public boolean exists(final String path) {
        return exists(path, hasListeners(path));
    }

    private boolean hasListeners(String path) {
        Set<ZkDataListener> dataListeners = dataListener.get(path);
        if (dataListeners != null && dataListeners.size() > 0) {
            return true;
        }
        Set<ZkChildListener> childListeners = childListener.get(path);
        if (childListeners != null && childListeners.size() > 0) {
            return true;
        }
        return false;
    }

    public List<String> getChildren(String path) {
        return getChildren(path, hasListeners(path));
    }

    protected List<String> getChildren(final String path, final boolean watch) {
        return retryUntilConnected(new Callable<List<String>>() {
            @Override
            public List<String> call() throws Exception {
                return zkConnection.getChildren(path, watch);
            }
        });
    }

    public boolean delete(final String path) {
        return delete(path, -1);
    }

    public boolean delete(final String path, final int version) {
        try {
            retryUntilConnected(new Callable<Object>() {

                @Override
                public Object call() throws Exception {
                    zkConnection.delete(path, version);
                    return null;
                }
            });

            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public <T extends Object> T readData(String path) {
        return (T) readData(path, false);
    }

    @SuppressWarnings("unchecked")
    public <T extends Object> T readData(String path, boolean returnNullIfPathNotExists) {
        T data = null;
        try {
            data = (T) readData(path, null);
        } catch (Exception e) {
            if (!returnNullIfPathNotExists) {
                throw e;
            }
        }
        return data;
    }

    @SuppressWarnings("unchecked")
    public <T extends Object> T readData(String path, Stat stat) {
        return (T) readData(path, stat, hasListeners(path));
    }

    @SuppressWarnings("unchecked")
    protected <T extends Object> T readData(final String path, final Stat stat, final boolean watch) {
        byte[] data = retryUntilConnected(new Callable<byte[]>() {

            @Override
            public byte[] call() throws Exception {
                return zkConnection.readData(path, stat, watch);
            }
        });
        return null;
    }

    public void writeData(String path, Object object) {
        writeData(path, object, -1);
    }



    public void writeData(final String path, Object datat, final int expectedVersion) {
        writeDataReturnStat(path, datat, expectedVersion);
    }

    public Stat writeDataReturnStat(final String path, Object datat, final int expectedVersion) {
        final byte[] data = String.valueOf(datat).getBytes();
        return (Stat) retryUntilConnected(new Callable<Object>() {

            @Override
            public Object call() throws Exception {
                Stat stat = zkConnection.writeDataReturnStat(path, data, expectedVersion);
                return stat;
            }
        });
    }
}
