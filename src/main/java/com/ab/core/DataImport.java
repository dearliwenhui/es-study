package com.ab.core;

import com.ab.core.utils.HttpClientUtil;
import com.ab.core.utils.RedisUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 从接口中获取数据，放到HBase和redis
 *
 * @version: 0.0.1
 * @author: liwenhui
 * @createTime: 2022-11-27 10:44
 **/
@Slf4j
public class DataImport {
    private static ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        try(Connection conn = DriverManager.getConnection("jdbc:phoenix:106.75.171.152:2181");
            Jedis jedis =  RedisUtil.getJedis()) {
            //建表
            //createTable(conn);
            String s1 = HttpClientUtil.doGet("https://www.mxnzp.com/api/news/list?typeId=518&page=3&app_id=zoyiylmslogtpm8w&app_secret=WGpSUU1QL3VyVXBVTVdFc2dmMDFIZz09");
            HashMap hashMap = objectMapper.readValue(s1, HashMap.class);
            List<Map<String, String>> dataList = (List<Map<String, String>>) hashMap.get("data");
            for (Map<String, String> dataMap : dataList) {
                //接口限制了qps，所以休眠1s
                TimeUnit.MILLISECONDS.sleep(1000);
                String newsId = dataMap.get("newsId");
                String contentString = HttpClientUtil.doGet("https://www.mxnzp.com/api/news/details?app_id=zoyiylmslogtpm8w&app_secret=WGpSUU1QL3VyVXBVTVdFc2dmMDFIZz09&newsId=" + newsId);
                HashMap contentHashMap = objectMapper.readValue(contentString, HashMap.class);
                if (1 != ((Integer) contentHashMap.get("code"))) {
                    continue;
                }
                //文章ID作为HBase的Rowkey和ES的ID
                Map<String, String> contentMap = (Map<String, String>) contentHashMap.get("data");
                String id = contentMap.get("docid");
                String title = contentMap.get("title");
                String author = contentMap.get("source");
                String describe = dataMap.get("digest");
                String content = contentMap.get("content");
                String time = contentMap.get("ptime");

                //逻辑上需要将HBase操作和Redis操作放到一个事务里面，但是在代码层面不支持
                //可以考虑，使用try catch来操作
                try {
                    //将数据入库到HBase
                    try (PreparedStatement ps = conn.prepareStatement("upsert into article values (?,?,?,?,?,?)")) {
                        ps.setString(1, id);
                        ps.setString(2, title);
                        ps.setString(3, author);
                        ps.setString(4, describe);
                        ps.setString(5, content);
                        ps.setString(6, time);
                        ps.executeUpdate();
                        conn.commit();
                    }
                    //将Rowkey保存到redis中
                    jedis.lpush("l_article_ids", id);
                } catch (Exception e) {
                    //注意：由于hbase的put操作属于幂等操作，多次操作，对最终的结果没有影响，所以不需要额外处理
                    //todo 进行异常处理
                    log.error("数据添加失败: ", e);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }

    private static void createTable(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS article(\n" +
                    "\tid varchar primary key,\n" +
                    "\ttitle varchar,\n" +
                    "\tauthor varchar,\n" +
                    "\tdescribe varchar,\n" +
                    "\tcontent varchar,\n" +
                    "\ttime varchar\n" +
                    ")");
            conn.commit();
        }

    }
}
