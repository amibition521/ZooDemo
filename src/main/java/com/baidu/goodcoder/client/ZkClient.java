package com.baidu.goodcoder.client;

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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Slf4j
@Component
public class ZkClient implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(ZkClient.class);

    private static final int DEFAULT_TIME = 1000;
    private static final int DEFAULT_TIMEOUT =  5 * DEFAULT_TIME;
    private static final int DEFAULT_RETRY_TIMES = 5;
    private static final String ZOOKEEPER_SERVER_URL = "172.17.0.3:2181";
    private static final String DEFAULT_NAMESPACE = "nieyx";


    private CuratorFramework zkCustor;

    @Override
    public void afterPropertiesSet() throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(DEFAULT_TIME, DEFAULT_RETRY_TIMES);

        zkCustor = CuratorFrameworkFactory.builder().connectString(ZOOKEEPER_SERVER_URL)
                .sessionTimeoutMs(DEFAULT_TIMEOUT)
                .retryPolicy(retryPolicy)
                .namespace(DEFAULT_NAMESPACE)
                .build();

        try {
            zkCustor.start();
            logger.debug("Create ZkClient success");
        } catch (Exception e) {
            e.printStackTrace();
            logger.debug("Create ZkClient fail");
        }
    }

    /**
     * 创建节点
     *
     * @param path 路径
     * @param data 数据
     * @return true 创建成功，否则失败
     */
    public boolean create(String path, String data) {
        if (StringUtils.isEmpty(path) || StringUtils.isEmpty(data)) {
            logger.debug("Create node path is empty or data is empty");
            throw new IllegalArgumentException("Create path is empty");
        }
        if (!path.contains("/")){
            throw new IllegalStateException(" Path must start with / character");
        }
        try {
            zkCustor.create().creatingParentContainersIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                    .forPath(path, data.getBytes());
            logger.debug("Create node success");
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            logger.debug("Create node fail");
        }

        return false;
    }

    /**
     * 删除节点
     *
     * @param path 路径
     * @return true 删除成功，否则失败
     */
    public boolean delete(String path) {
        if (StringUtils.isEmpty(path)) {
            logger.debug("Delete node fail");
            throw new IllegalArgumentException("Delete path is empty");
        }
        if (!path.contains("/")){
            throw new IllegalStateException(" Path must start with / character");
        }
        try {
            if (!isExist(path)) {
                logger.error("node is not exist");
                return false;
            }
            zkCustor.delete().guaranteed().withVersion(-1).forPath(path);
            logger.debug("Delete node success");
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            logger.debug("Delete node fail");
        }
        return false;
    }

    /**
     * 节点是否存在
     *
     * @param path 节点路径
     * @return true 存在，否则不存在
     */
    public boolean isExist(String path) {
        if (StringUtils.isEmpty(path)) {
            throw new IllegalArgumentException("Exist path is empty");
        }
        if (!path.contains("/")){
            throw new IllegalStateException(" Path must start with / character");
        }
        Stat stat = null;
        try {
            stat = zkCustor.checkExists().forPath(path);
            logger.debug("Exist is true");
            return stat != null;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 更新节点
     * @param path
     * @param data 数据
     * @return
     */
    public boolean update(String path, String data) {
        if (StringUtils.isEmpty(path) || StringUtils.isEmpty(data)) {
            throw new IllegalArgumentException("update path is empty");
        }
        if (!path.contains("/")){
            throw new IllegalStateException(" Path must start with / character");
        }

        try {
            if (!isExist(path)) {
                logger.error("node is not exist");
                return false;
            }
            zkCustor.setData().forPath(path, data.getBytes());
            logger.debug("update node success");
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            logger.debug("update node fail");
        }
        return false;
    }

    /**
     * 获取节点
     * @param path
     * @return
     */
    public String getNode(String path) {
        if (StringUtils.isEmpty(path)) {
            throw new IllegalArgumentException("path is empty");
        }
        try {
            if (isExist(path)) {
                logger.debug("Node is exist");
                return new String(zkCustor.getData().forPath(path));
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.debug("Node is not exist");
        }

        return null;
    }

    /**
     * 观察节点数据变化
     * @param src 源节点
     * @param dst 目标节点
     * @return
     */
    public boolean watchNode(String src, String dst) {
        NodeCache cache = new NodeCache(zkCustor, src);
        NodeCacheListener listener = () -> {
            ChildData data = cache.getCurrentData();
            if (data != null) {
                this.update(dst, new String(cache.getCurrentData().getData()));
                logger.debug("update dst node");
            }
        };
        cache.getListenable().addListener(listener);
        try {
            cache.start();
            logger.debug("update dst node success");
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            logger.debug("update dst node fail");
        }
        return false;
    }


}
