package com.example.demo;

import com.example.demo.bean.ResponseResult;
import org.junit.Assert;
import org.junit.Test;

public class ResponseResultTest {

    @Test
    public void testResult(){
        ResponseResult result = new ResponseResult();
        result.setCode(200);
        Assert.assertEquals(Integer.valueOf(200), Integer.valueOf(result.getCode()));
    }
}
