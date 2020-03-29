package com.zhang.gmall0826.gmall0826publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

//data生产set、get方法
//AllArgsConstructor生成全部的构造器
@Data
@AllArgsConstructor
public class Option {
    String name;
    Double value;
}
