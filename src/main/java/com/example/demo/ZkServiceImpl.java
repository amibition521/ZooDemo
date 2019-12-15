package com.example.demo;

import com.example.demo.client.ZkClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.springframework.stereotype.Service;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Service
@Slf4j
public class ZkServiceImpl implements ZkService {

    private ZkClient zk;

    @Override
    public void create(String path, String data)  {
        zk.create(path, data);
    }

    @Override
    public void delete(String path) {
        zk.delete(path);
    }

    @Override
    public void update(String path, String data) {
        zk.update(path, data);
    }

    @Override
    public boolean isExist(String path)  {
        return zk.isExist(path);
    }

    @Override
    public String getNode(String path) {
        return zk.getNode(path);
    }

}
