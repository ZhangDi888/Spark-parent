package com.zhang.gmall0826.gmall0826publisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
/**
* @Description: 通过MapperScan告诉spring扫描mapper接口，
 * 通过application.properties配置文件告诉spring，mapper的实现类在哪
 * DauMapper.xml会在文件里，声明mapper实现的接口是哪个，同时通过表名称
 * 和返回值类型，和接口的方法对接上，然后执行sql查询
 *
 * 服务层对持久层(mapper)进行包装，调取持久层的数据，在进行细化
* @Date: 2020/2/10
*/

@SpringBootApplication
//告诉框架要扫描Mapper，应该扫描哪个目录；basePackages是Mapper的目录
@MapperScan(basePackages = "com.zhang.gmall0826.gmall0826publisher.mapper")
public class Gmall0826PublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(Gmall0826PublisherApplication.class, args);
    }

}
