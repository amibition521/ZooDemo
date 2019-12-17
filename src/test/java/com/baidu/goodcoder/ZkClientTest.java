package com.baidu.goodcoder;

import com.baidu.goodcoder.client.ZkClient;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest(classes = DemoApplication.class)
public class ZkClientTest {

    @Autowired
    private ZkClient zkClient;

    @Test
    public void testExist(){
        try {
            boolean exist = zkClient.isExist("/node1");
            Assert.assertEquals(Boolean.FALSE, exist);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void testCreate() {
        try {
            boolean result = zkClient.create("/node1", "node_data1");
            Assert.assertEquals(Boolean.TRUE, result);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void testDelete() {
        try {
            boolean result = zkClient.delete("/node1");
            Assert.assertEquals(Boolean.TRUE, result);
        } catch (Exception e){
            e.printStackTrace();
        }
    }


    @Test
    public void testUpdate() {
        try {
            boolean result = zkClient.update("/node1", "node_data_new");
            Assert.assertEquals(Boolean.TRUE, result);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void testGet() {
        try {
            String result = zkClient.getNode("/node1");
            Assert.assertEquals("node_data1", result);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void testWatch() throws InterruptedException {
        String srcPath = "/zk_path1";
        String dstPath = "/zk_path2";

        zkClient.create(srcPath, "data1");
        zkClient.create(dstPath, "data2");

        try {
            zkClient.watchNode(srcPath, dstPath);
        }catch (Exception e){
            e.printStackTrace();
        }

        zkClient.update(srcPath, "data3");
        Thread.sleep(200);
        String res = zkClient.getNode(dstPath);
        Assert.assertEquals("data3", res);

        zkClient.delete(srcPath);
        zkClient.delete(dstPath);
    }

}
