package com.zhang.gmall0826.gmall0826publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class Stat {
    String title;
    List<Option> options;
}
