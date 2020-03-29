package com.zhang.gmall0826.gmall0826publisher.service;

import java.util.Map;

public interface PublisherService {

    public Long getDauTotal(String date);

    public Map getDauHourCount(String date);

    public Double getOrderTotalAmount(String date);

    public Map getOrderHourTotalAmount(String date);

    public Map getSaleDetail(String date, String keyWord, int pageNo, int pageSize);
}
