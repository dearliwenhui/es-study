package com.ab.core;

import com.ab.core.utils.EsUtil;
import com.ab.core.utils.RedisUtil;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;

import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 在ES中对HBase中的数据建立索引
 *
 * @version: 0.0.1
 * @author: liwenhui
 * @createTime: 2022-11-27 16:58
 **/
@Slf4j
public class DataIndex {

    public static void main(String[] args) {
        List<String> rowKeyList = null;
        Jedis jedis = null;
        try (Connection conn = DriverManager.getConnection("jdbc:phoenix:106.75.171.152:2181")) {
            jedis = RedisUtil.getJedis();
            do {
                //brpop命令说明:https://www.redis.net.cn/order/3578.html
                rowKeyList = jedis.brpop(3, "l_article_ids");
                if (rowKeyList != null) {
                    String rowKey = rowKeyList.get(1);
                    Map<String, String> stringStringMap = queryByRowKey(conn,rowKey);
                    EsUtil.addIndex("article", rowKey, stringStringMap);
                }
            } while (rowKeyList != null);


        } catch (Exception e) {
            log.error("ES建立index失败：", e);
            //在这里可以考虑把获取出来的rowKey再push到Redis中，这样可以保证数据不丢
            //todo 加个重试次数，达到次数可以入库或其他操作
            if (rowKeyList != null) {
                jedis.rpush("l_article_ids", rowKeyList.get(1));
            }
        }finally {
            if (jedis != null) {
                jedis.close();
            }

        }
    }

    private static Map<String, String> queryByRowKey(Connection conn, String rowKey) throws Exception {
        HashMap<String, String> contentMap = new HashMap<>(1);
        ResultSet rs = null;
        try (PreparedStatement ps = conn.prepareStatement("select * from article where id = ?");

        ) {
            ps.setString(1, rowKey);
            rs = ps.executeQuery();
            while (rs.next()) {
                String id = rs.getString(1);
                String title = rs.getString(2);
                String author = rs.getString(3);
                String describe = rs.getString(4);
                String content = rs.getString(5);
                String time = rs.getString(6);
//                contentMap.put("id", id);
                contentMap.put("title", title);
                contentMap.put("author", author);
                contentMap.put("describe", describe);
                contentMap.put("content", content);
                contentMap.put("time", time);
            }
        } finally {
            if (rs != null || !rs.isClosed()) {
                rs.close();
            }
        }
        return contentMap;
    }

}
