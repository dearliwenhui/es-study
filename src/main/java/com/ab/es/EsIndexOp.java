package com.ab.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;

/**
 *  针对ES中索引库的操作
 * @version: 0.0.1
 * @author: liwenhui
 * @createTime: 2022-11-18 22:25
 **/
public class EsIndexOp {

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = getRestHighLevelClient();
        //创建index
        //createIndex(client);
        //删除index
        //deleteIndex(client);
        close(client);

    }

    private static void deleteIndex(RestHighLevelClient client) throws IOException {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("java_test");
        client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
    }

    private static void createIndex(RestHighLevelClient client) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest("java_test");
        //配置index的配置信息
        request.settings(Settings.builder()
                .put("index.number_of_shards", 2)
                .put("index.number_of_replicas", 1)
        );
        //执行
        client.indices().create(request, RequestOptions.DEFAULT);
    }

    public static void close(RestHighLevelClient client) {
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static RestHighLevelClient getRestHighLevelClient() {
        //获取RestClient连接
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("myhost", 9200, "http")));
        return client;
    }

}
