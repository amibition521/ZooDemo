package com.baidu.goodcoder.service;

public interface ZkService {

    void create(String path, String data);

    boolean isExist(String path);

    String getNode(String path);
    
    void delete(String path);

    void update(String path, String data);

}
