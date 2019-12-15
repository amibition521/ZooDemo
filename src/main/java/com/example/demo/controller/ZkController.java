package com.example.demo.controller;

import com.example.demo.ZkService;
import com.example.demo.bean.ResponseResult;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.validator.constraints.pl.REGON;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController(value = "/zk")
public class ZkController {
    @Autowired
    private ZkService zkService;

    @ResponseBody
    @RequestMapping(value = "/create", method = RequestMethod.POST)
    public ResponseEntity<ResponseResult> create(@RequestParam("path") String path,
                                                 @RequestParam("data") String data) {
        ResponseResult result = new ResponseResult();
        try {
            zkService.create(path, data);
        } catch (Exception e){
            e.printStackTrace();
        }
        result.setCode(220);
        result.setMsg("success");

        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @ResponseBody
    @RequestMapping(value = "/delete", method = RequestMethod.GET)
    public ResponseEntity<ResponseResult> delete(@RequestParam("path") String path){
        ResponseResult result = new ResponseResult();
        try {
            zkService.delete(path);
        } catch (Exception e){
            e.printStackTrace();
        }
        result.setCode(220);
        result.setMsg("success");
        return new ResponseEntity<>(result, HttpStatus.OK);
    }
    @ResponseBody
    @RequestMapping(value = "/get_node", method = RequestMethod.GET)
    public ResponseEntity<ResponseResult> getNode(@RequestParam("path") String path){
        ResponseResult result = new ResponseResult();
        try {
            zkService.getNode(path);
        } catch (Exception e){
            e.printStackTrace();
        }
        result.setCode(220);
        result.setMsg("success");
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @ResponseBody
    @RequestMapping(value = "/update", method = RequestMethod.POST)
    public ResponseEntity<ResponseResult> update(@RequestParam("path") String path, @RequestParam("data") String data){
        ResponseResult result = new ResponseResult();
        try {
            zkService.update(path, data);
        } catch (Exception e){
            e.printStackTrace();
        }
        result.setCode(220);
        result.setMsg("success");
        return new ResponseEntity<>(result, HttpStatus.OK);
    }


}
