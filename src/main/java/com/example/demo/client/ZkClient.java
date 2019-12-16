package com.example.demo.client;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.StringUtils;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ZkClient implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(ZkClient.class);

    private static final int DEFAULT_TIME = 1000;
    private static final int DEFAULT_RETRY_TIMES = 5;
    private static final String ZOOKEEPER_SERVER_URL = "127.0.0.1:2181";
    private static final String DEFAULT_NAMESPACE = "nieyx";


    private CuratorFramework zkCustor;

    @Override
    public void afterPropertiesSet() throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(DEFAULT_TIME, DEFAULT_RETRY_TIMES);

        zkCustor = CuratorFrameworkFactory.builder().connectString(ZOOKEEPER_SERVER_URL)
                .sessionTimeoutMs(DEFAULT_TIME)
                .retryPolicy(retryPolicy)
                .namespace(DEFAULT_NAMESPACE)
                .build();

        zkCustor.start();
    }

    public boolean create(String path, String data) {
        if (StringUtils.isEmpty(path) || StringUtils.isEmpty(data)) {
            return false;
        }
        try {
            zkCustor.create().creatingParentContainersIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                    .forPath(path, data.getBytes());
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public boolean delete(String path) {
        if (StringUtils.isEmpty(path)) {
            return false;
        }
        try {
            if (!isExist(path)) {
                logger.error("node is not exist");
                return false;
            }
            zkCustor.delete().guaranteed().withVersion(-1).forPath(path);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public boolean isExist(String path) {
        if (StringUtils.isEmpty(path)) {
            return false;
        }
        Stat stat = null;
        try {
            stat = zkCustor.checkExists().forPath(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return stat != null;
    }

    public boolean update(String path, String data) {
        if (StringUtils.isEmpty(path) || StringUtils.isEmpty(data)) {
            return false;
        }

        try {
            if (!isExist(path)) {
                logger.error("node is not exist");
                return false;
            }
            zkCustor.setData().forPath(path, data.getBytes());
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public String getNode(String path) {
        if (StringUtils.isEmpty(path)) {
            return null;
        }
        try {
            if (zkCustor.checkExists().forPath(path) != null) {
                return new String(zkCustor.getData().forPath(path));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public boolean watchNode(String src, String dst) {
        NodeCache cache = new NodeCache(zkCustor, "/zk_path1");
        NodeCacheListener listener = () -> {
            ChildData data = cache.getCurrentData();
            if (data != null) {
                this.update(dst, new String(cache.getCurrentData().getData()));
            }
        };
        cache.getListenable().addListener(listener);
        try {
            cache.start();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }


}
