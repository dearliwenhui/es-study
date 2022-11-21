package com.ab.es;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 针对ES中索引数据的操作
 * @version: 0.0.1
 * @author: liwenhui
 * @createTime: 2022-11-18 23:37
 **/
public class EsDataOp {

    public static final String INDICES_NAME = "emp";

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = EsIndexOp.getRestHighLevelClient();

        //创建索引
        //通过json创建
//        addIndexByJson(client);
        //通过map创建
//        addIndexByMap(client);

        //查询索引
//        getIndexById(client, "10");
//        getIndexByField(client);

        //更新索引
        //注意：可以使用创建索引直接完整更新已存在的数据
        //更新部分字段
//        updateIndexByPart(client);
        //删除索引
//        deleteIndex(client);

        //Bulk批量操作
        bulkIndex(client);

        EsIndexOp.close(client);
    }

    private static void bulkIndex(RestHighLevelClient client) throws IOException {
        BulkRequest request = new BulkRequest();
        //增加index，可以使k v的方式也可以使用map的方式
        request.add(new IndexRequest(INDICES_NAME).id("20")
                .source(XContentType.JSON, "field1", "value1", "field2", "value2"));
        //如果id不存在，删除也不会报错
        request.add(new DeleteRequest(INDICES_NAME).id("10"));
        //更新数据
        request.add(new UpdateRequest(INDICES_NAME, "20").doc("field1", "v1"));
        //如果id为100的数据不存在，这条命令执行时会失败
        request.add(new UpdateRequest(INDICES_NAME, "100").doc("age", 30));
        //更新数据不存在的字段，会增加进去
        request.add(new UpdateRequest(INDICES_NAME, "20").doc("newField", "newValue"));
        //执行
        BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
        //如果Bulk中个别语句出错不会导致整个Bulk失败，不影响后续的操作执行，所以可以在这里判断一下是否有返回执行失败的信息
        for (BulkItemResponse bulkItemResponse : bulkResponse) {
            if (bulkItemResponse.isFailed()) {
                System.out.println("Bulk中出现了异常：" + bulkItemResponse.getFailureMessage());
            }
        }
    }

    private static void deleteIndex(RestHighLevelClient client) throws IOException {
        DeleteRequest request = new DeleteRequest(INDICES_NAME, "10");
        client.delete(request, RequestOptions.DEFAULT);
    }

    private static void updateIndexByPart(RestHighLevelClient client) throws IOException {
        UpdateRequest request = new UpdateRequest(INDICES_NAME, "10");
        String json = "{\"age\":18}";
        request.doc(json, XContentType.JSON);
        //也可以使用map更新
        client.update(request, RequestOptions.DEFAULT);
    }

    private static void getIndexByField(RestHighLevelClient client) throws IOException {
        GetRequest request = new GetRequest(INDICES_NAME,"10");
        String[] includes = {"name"};//指定查询结果包含哪些字段
        String[] excludes = Strings.EMPTY_ARRAY;//指定查询结果过滤哪些字段
        //fetchSource：表示是否获取source字段
        // 过滤数据
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
        request.fetchSourceContext(fetchSourceContext);
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        //通过response获取index、id、文档详细内容（source）
        String index = response.getIndex();
        //如果没有查询到文档数据，则isExists返回false
        if (response.isExists()) {
            //获取json字符串格式的文档结果
            String sourceAsString = response.getSourceAsString();
            System.out.println(sourceAsString);
            //获取map格式的文档结果
            Map<String, Object> sourceAsMap = response.getSourceAsMap();
            System.out.println(sourceAsMap);
        }
    }

    private static void getIndexById(RestHighLevelClient client, String id) throws IOException {
        GetRequest request = new GetRequest(INDICES_NAME, id);
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        //通过response获取index、id、文档详细内容（source）
        String index = response.getIndex();
        //如果没有查询到文档数据，则isExists返回false
        if (response.isExists()) {
            //获取json字符串格式的文档结果
            String sourceAsString = response.getSourceAsString();
            System.out.println(sourceAsString);
            //获取map格式的文档结果
            Map<String, Object> sourceAsMap = response.getSourceAsMap();
            System.out.println(sourceAsMap);
        }
    }

    private static void addIndexByMap(RestHighLevelClient client) throws IOException {
        IndexRequest request = new IndexRequest(INDICES_NAME);
        request.id("11");
        HashMap<String, Object> map = new HashMap<>();
        map.put("name", "tom");
        map.put("age", 40);
        request.source(map);
        client.index(request, RequestOptions.DEFAULT);
    }

    private static void addIndexByJson(RestHighLevelClient client) throws IOException {
        IndexRequest request = new IndexRequest(INDICES_NAME);
        request.id("10");
        String json = "{\n" +
                "  \"name\": \"Maggie\",\n" +
                "  \"age\": 31\n" +
                "}";
        request.source(json, XContentType.JSON);
        //设置routing，routing参数一样，数据进入到同一个分片中
        request.routing("class1");
        client.index(request, RequestOptions.DEFAULT);
    }
}
