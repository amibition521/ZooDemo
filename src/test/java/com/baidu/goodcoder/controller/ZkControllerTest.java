package com.baidu.goodcoder.controller;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.mock.web.MockHttpSession;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MockMvcBuilder;
import org.springframework.test.web.servlet.RequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ZkControllerTest {
    @Autowired
    private WebApplicationContext context;
    private MockMvc mvc;
    private MockHttpSession session;

    @Before
    public void initMvc() {
        mvc = MockMvcBuilders.webAppContextSetup(context).build();
        session = new MockHttpSession();
    }

    @Test
    public void testCreate() throws Exception {
        RequestBuilder requestBuilder = MockMvcRequestBuilders.post("/zk/create").param("path", "/node1")
                .param("data","node_data1");
        mvc.perform(requestBuilder).andExpect(MockMvcResultMatchers.status().isOk())
                .andDo(MockMvcResultHandlers.print());
    }

    @Test
    public void testDelete() throws Exception {
        RequestBuilder requestBuilder = MockMvcRequestBuilders.get("/zk/delete").param("path", "/node1");
        mvc.perform(requestBuilder).andExpect(MockMvcResultMatchers.status().isOk())
                .andDo(MockMvcResultHandlers.print());
    }

    @Test
    public void testGet() throws Exception {
        RequestBuilder requestBuilder = MockMvcRequestBuilders.get("/zk/get").param("path", "/node1");
        mvc.perform(requestBuilder).andExpect(MockMvcResultMatchers.status().isOk())
                .andDo(MockMvcResultHandlers.print());
    }

    @Test
    public void testUpdate() throws Exception {
        RequestBuilder requestBuilder = MockMvcRequestBuilders.post("/zk/update").param("path", "/node1")
                .param("data","node_data_new");
        mvc.perform(requestBuilder).andExpect(MockMvcResultMatchers.status().isOk())
                .andDo(MockMvcResultHandlers.print());
    }






}
