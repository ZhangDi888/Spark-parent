package com.zhang.gmall0826.gmall0826publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
public class Result {
    Long total;
    List<Map> detail;
    List<Stat> stat;
}
