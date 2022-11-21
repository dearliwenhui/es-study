package com.ab.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.io.IOException;

/**
 * routing路由功能的使用
 * @version: 0.0.1
 * @author: liwenhui
 * @createTime: 2022-11-21 21:17
 **/
public class EsRoutingOp {

    private static final String INDICES_NAME = "rout";

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = getRestHighLevelClient();
        SearchRequest searchRequest = new SearchRequest();
        //指定索引库，支持指定一个或者多个，也支持通配符，例如：user*
        searchRequest.indices(INDICES_NAME);

        //指定分片查询方式，这种hardcode方式不友好
        //searchRequest.preference("_shards:0");

        //指定路由参数，会自动计算_shards的值
        searchRequest.routing("class1");

        //执行查询操作
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);


        //返回的是第一个hits，包含total、实际数据
        SearchHits hits = searchResponse.getHits();
        //获取数据总量
        long numHits = hits.getTotalHits().value;
        System.out.println("数据总数" + numHits);
        //获取具体内容
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
        }
        close(client);
    }

    private static void close(RestHighLevelClient client) {
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static RestHighLevelClient getRestHighLevelClient() {
        //获取RestClient连接
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("myhost", 9200, "http")));
        return client;
    }


}
