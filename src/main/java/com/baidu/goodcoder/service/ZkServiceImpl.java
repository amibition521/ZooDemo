package com.baidu.goodcoder.service;

import com.baidu.goodcoder.client.ZkClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

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
