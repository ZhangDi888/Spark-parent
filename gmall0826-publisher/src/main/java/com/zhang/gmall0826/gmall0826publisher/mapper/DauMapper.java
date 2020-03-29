package com.zhang.gmall0826.gmall0826publisher.mapper;

import javafx.scene.control.Tab;
import org.springframework.context.annotation.Description;

import java.util.List;
import java.util.Map;

public interface DauMapper {

    /**
     * @Description: 请求进来先进controller，controller会调服务层，
     * 服务层再调数据层（mapper）
     *
     * 这些未实现的方法会通过resources的mapper.xml和phoeinx产生联系，
     * 读取phoeinx的sql实现查询
     * @Author: Mr.Z
     * @Date: 2020/2/10
     *
    */
    //通过参数 日期，调取phoenix 得到结果
    public Long selectDauTotal(String date);

    //通过参数，调取结果 list代表很多行，map代表每行里的数据，k代表字段名，v代表字段值
    public List<Map> selectDauHourCount(String date);

}
