package com.baidu.goodcoder.controller;

import com.baidu.goodcoder.bean.NodeBean;
import com.baidu.goodcoder.bean.ResponseResult;
import com.baidu.goodcoder.service.ZkService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping(value = "/zk")
public class ZkController {
    @Autowired
    private ZkService zkService;

    @ResponseBody
    @PostMapping(value = "/create", consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity<ResponseResult> create(@RequestBody NodeBean bean) {
        ResponseResult result = new ResponseResult();
        try {
            zkService.create(bean.getPath(), bean.getData());
        } catch (Exception e){
            e.printStackTrace();
        }
        result.setCode(200);
        result.setMsg("success");

        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @ResponseBody
    @GetMapping(value = "/delete")
    public ResponseEntity<ResponseResult> delete(@RequestParam("path") String path){
        ResponseResult result = new ResponseResult();
        try {
            if (zkService.isExist(path)){
                zkService.delete(path);
            }
        } catch (Exception e){
            e.printStackTrace();
        }
        result.setCode(200);
        result.setMsg("success");
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @ResponseBody
    @GetMapping(value = "/get_node")
    public ResponseEntity<ResponseResult> getNode(@RequestParam("path") String path){
        ResponseResult result = new ResponseResult();
        try {
            zkService.getNode(path);
        } catch (Exception e){
            e.printStackTrace();
        }
        result.setCode(200);
        result.setMsg("success");
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @ResponseBody
    @PostMapping(value = "/update", consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity<ResponseResult> update(@RequestBody NodeBean bean){
        ResponseResult result = new ResponseResult();
        try {
            zkService.update(bean.getPath(), bean.getData());
        } catch (Exception e){
            e.printStackTrace();
        }
        result.setCode(200);
        result.setMsg("success");
        return new ResponseEntity<>(result, HttpStatus.OK);
    }


}
