package com.example;

import java.util.List;

public interface ZkChildListener {

    public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception;

}
