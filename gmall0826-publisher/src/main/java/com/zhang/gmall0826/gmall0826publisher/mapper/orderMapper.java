package com.zhang.gmall0826.gmall0826publisher.mapper;

import java.util.List;
import java.util.Map;

public interface orderMapper {

    //通过日期获取总交易额
    public Double selectOrderAmount(String date);

    //通过日期获取每小时成交额
    public List<Map> selectOrderHourAmount(String date);
}
