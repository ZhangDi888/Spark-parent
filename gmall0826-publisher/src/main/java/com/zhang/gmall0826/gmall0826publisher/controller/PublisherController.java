package com.zhang.gmall0826.gmall0826publisher.controller;

import com.alibaba.fastjson.JSON;
import com.zhang.gmall0826.gmall0826publisher.bean.Option;
import com.zhang.gmall0826.gmall0826publisher.bean.Result;
import com.zhang.gmall0826.gmall0826publisher.bean.Stat;
import com.zhang.gmall0826.gmall0826publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

//RestController使用的效果是将方法返回的对象直接在浏览器上展示成json格式
@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;

    //@RequestParam("date")是标准传参的写法
    @GetMapping("realtime-total")
    public String getRealTimeTotal(@RequestParam("date") String date){
        List<Map> totalList = new ArrayList<>();

        Map dauMap = new HashMap();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",publisherService.getDauTotal(date));

        totalList.add(dauMap);

        Map new_Map = new HashMap();
        new_Map.put("id","new_mid");
        new_Map.put("name","新增设备");
        new_Map.put("value","233");
        totalList.add(new_Map);

        Map orderMap = new HashMap();
        orderMap.put("id","order_amount");
        orderMap.put("name","新增交易额");
        orderMap.put("value",publisherService.getOrderTotalAmount(date));
        totalList.add(orderMap);
        return JSON.toJSONString(totalList);
    }

    @GetMapping("realtime-hour")
    public String getRealTimeHourCount(@RequestParam("id") String id,@RequestParam("date") String date){

        if("dau".equals(id)){
            Map dauHourCount = publisherService.getDauHourCount(date);
            String yestorday = getYestorday(date);
            Map dauHourCount1 = publisherService.getDauHourCount(yestorday);
            Map<String,Map> hourMap = new HashMap<>();
            hourMap.put("today",dauHourCount);
            hourMap.put("yestorday",dauHourCount1);

            return JSON.toJSONString(hourMap);
        } else if ("order_amount".equals(id)){
            Map orderHourTotalAmount = publisherService.getOrderHourTotalAmount(date);
            String yestorday = getYestorday(date);
            Map ysAmount = publisherService.getOrderHourTotalAmount(yestorday);
            Map<String,Map> hourMap = new HashMap<>();
            hourMap.put("today",orderHourTotalAmount);
            hourMap.put("yestorday",ysAmount);
            return JSON.toJSONString(hourMap);
        }
        return null;
    }

    public String getYestorday(String dt){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        try {
            //将参数转换成日期格式
            Date parse = simpleDateFormat.parse(dt);
            //利用date的工具类，将日期减一
            Date yesterday = DateUtils.addDays(parse, -1);
            //日期格式转换成string然后返回
            String format = simpleDateFormat.format(yesterday);
            return format;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    @GetMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date") String date,@RequestParam("startpage")int startpage,
                                @RequestParam("size") int size, @RequestParam("keyWord") String keyWord){
        Map saleDetail = publisherService.getSaleDetail(date, keyWord, startpage, size);
        //调整结构 比例 年龄段 中文
        Map groupby_age = (Map)saleDetail.get("groupby_age");
        Long total = (Long)saleDetail.get("total");

        Long age_20Count=0L;
        Long age20_30Count=0L;
        Long age30_Count=0L;

        //对各个年龄范围的人数统计
        for (Object o : groupby_age.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            String ageStr = (String)entry.getKey();
            Long ageCount = (Long)entry.getValue();
            //拆包
            int age = Integer.parseInt(ageStr);
            if(age < 20){
                age_20Count += ageCount;
            } else if (age >= 20 && age < 30) {
                age20_30Count += ageCount;
            } else {
                age30_Count += ageCount;
            }
        }

        //比如：总共3个购买的人，其中2个20岁以下的，2/3=66.7
        //但如果用age_20Count/total则直接为0
        //用下面的方法：2*1000/3为666.666经过round方法四舍五入，为667在/10 为66.7
        double age_20rate = Math.round(age_20Count * 1000D / total) / 10D;
        double age20_30rate = Math.round(age20_30Count * 1000D / total) / 10D;
        double age30_rate = Math.round(age30_Count * 1000D / total) / 10D;
        List<Option> ageOptions = new ArrayList<>();
        ageOptions.add(new Option("20岁以下",age_20rate));
        ageOptions.add(new Option("20岁到30岁",age20_30rate));
        ageOptions.add(new Option("30岁以上",age30_rate));

        //性别
        Map groupby_gender = (Map) saleDetail.get("groupby_gender");

        Long maleCount = (Long)groupby_gender.get("M");
        Long femaleCount = (Long)groupby_gender.get("F");
        Double maleRate = Math.round(maleCount * 1000D / total)/10D;
        Double femaleRate = Math.round(femaleCount * 1000D / total)/10D;
        List<Option> genderOptions = new ArrayList<>();
        genderOptions.add(new Option("男",maleRate));
        genderOptions.add(new Option("女",femaleRate));

        List<Stat> statList = new ArrayList<>();
        statList.add(new Stat("用户年龄占比",ageOptions));
        statList.add(new Stat("用户性别占比",genderOptions));

        List<Map> detail = (List<Map>)saleDetail.get("detail");
        Result result = new Result(total, detail, statList);
        return JSON.toJSONString(result);
    }

}
