package com.zhang.gmall0826.gmall0826publisher.service.impl;

import com.zhang.gmall0826.common.constat.GmallConstant;
import com.zhang.gmall0826.gmall0826publisher.mapper.DauMapper;
import com.zhang.gmall0826.gmall0826publisher.mapper.orderMapper;
import com.zhang.gmall0826.gmall0826publisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.apache.avro.Schema;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Description: spring框架想让容器注入光自动注入也不行，需要在实现类添加service
 * @Date: 2020/2/11
 */
@Service
public class PublisherServiceImpl implements PublisherService {

    /**
     * @Description: 自动注入，不需要new，就可以获取类的方法
     * <p>
     * 接口直接可以用，是因为mybatis会帮你实现，找到DauMapper.xml构造新的实现类（就是sql）
     * 注入到dauMapper，所以需要添加Autowired
     * @Date: 2020/2/11
     */

    @Autowired
    DauMapper dauMapper;

    @Autowired
    orderMapper orderMapper;

    @Autowired
    JestClient jestClient;

    @Override
    public Long getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauHourCount(String date) {
        Map hashMap = new HashMap();
        //   maps ===> [{"loghour":"12","ct",500},{"loghour":"11","ct":400}......]
        List<Map> maps = dauMapper.selectDauHourCount(date);
        //转换结构 hashMap ==> {"12":500,"11":400,......}
        for (Map map : maps) {
            hashMap.put(map.get("logHour"), map.get("ct"));
        }
        return hashMap;
    }

    @Override
    public Double getOrderTotalAmount(String date) {
        return orderMapper.selectOrderAmount(date);
    }

    @Override
    public Map getOrderHourTotalAmount(String date) {
        List<Map> maps = orderMapper.selectOrderHourAmount(date);
        //转换格式
        Map map = new HashMap<>();

        for (Map map1 : maps) {
            //获取查询结果的字段
            map.put(map1.get("CREATE_HOUR"),map1.get("ORDER_AMOUNT"));
        }
        return map;
    }

    @Override
    public Map getSaleDetail(String date, String keyWord, int pageNo, int pageSize) {
        //通过这种可以实现，不过不建议这么写，太low
        /*String query = "{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"dt\": \"2020-02-20\"\n" +
                "        }\n" +
                "      },\n" +
                "      \"must\": [\n" +
                "        {\n" +
                "          \"match\": {\n" +
                "            \"sku_name\": {\n" +
                "              \"query\": \"小米手机\",\n" +
                "              \"operator\": \"and\"\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  },\n" +
                "  \"aggs\": {\n" +
                "    \"groupby_age\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"user_age\",\n" +
                "        \"size\": 120\n" +
                "      }\n" +
                "    },\n" +
                "    \"groupby_gender\":{\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"user_gender\",\n" +
                "        \"size\": 2\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"from\": 0\n" +
                "  ,\"size\": 5\n" +
                "}";*/
        //建立构造器
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //查询条件
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("dt",date));
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name",keyWord).operator(MatchQueryBuilder.Operator.AND));
        searchSourceBuilder.query(boolQueryBuilder);

        //聚合，年龄
        TermsBuilder ageBuilder = AggregationBuilders.terms("groupby_age").field("user_age").size(120);
        searchSourceBuilder.aggregation(ageBuilder);

        //性别
        TermsBuilder size = AggregationBuilders.terms("groupby_gender").field("user_gender").size(2);
        searchSourceBuilder.aggregation(size);

        //分页
        int rowNo = (pageNo - 1) * pageSize;//这个是行号，公式：(页码-1)*页大小
        searchSourceBuilder.from(rowNo);
        searchSourceBuilder.size(pageSize);

        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_SALE).addType("_doc").build();
        Map resultMap = new HashMap();//明细，总条数，年龄的聚合效果，性别的聚合结果
        try {
            SearchResult searchResult = jestClient.execute(search);
            //明细数据
            ArrayList<Map> detailList = new ArrayList<>();
            //Map.Class,将取出来的结果以map的形式封装（也可以做一个和detail字段相同的bean封装）
            List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
            for (SearchResult.Hit<Map, Void> hit : hits) {
                detailList.add(hit.source);//source就是map的内容
            }
            //总条数
            Long total = searchResult.getTotal();
            //年龄的聚合效果
            Map ageMap = new HashMap();
            List<TermsAggregation.Entry> groupby_age = searchResult.getAggregations().getTermsAggregation("groupby_age").getBuckets();
            //上面结果是键值对，向有年龄+人数的组合，需要遍历
            for (TermsAggregation.Entry entry : groupby_age) {
                ageMap.put(entry.getKey(),entry.getCount());
            }
            //性别的聚合效果
            Map genderMap = new HashMap();
            List<TermsAggregation.Entry> groupby_gender = searchResult.getAggregations().getTermsAggregation("groupby_gender").getBuckets();
            for (TermsAggregation.Entry entry : groupby_gender) {
                genderMap.put(entry.getKey(),entry.getCount());
            }
            //处理结果返回
            resultMap.put("detail",detailList);
            resultMap.put("total",total);
            resultMap.put("groupby_age",groupby_age);
            resultMap.put("groupby_gender",groupby_gender);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return resultMap;
}
}