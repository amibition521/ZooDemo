# nieyuxinjava
简要说明

## 快速开始
如何构建、安装、运行

###代码设计

采用 Spring boot 框架，开发容易、部署简单。

### 模块说明

+ `controller` 模块主要负责 api 请求，包括：增、删、改、查等操作，创建操作、更新操作和删除操作采用 post 方式进行请求，查询操作采用 get 请求。

+ `service`模块采用接口隔离的原则，负责具体实现。
+ `client`模块封装了 `zookeeper` 的 `create`、`delete`、`update`、`get` 和 `watch`等方法 

### 配置模块

在配置文件中，主要包括：端口号，`zookeeper`的配置项和 watch 的监控地址。



## 测试
如何执行自动化测试

## 如何贡献
贡献patch流程、质量要求
