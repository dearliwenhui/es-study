package com.ab.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;

/**
 * 聚合统计：统计每个学员的总成绩
 * @原始数据
 * curl -H "Content-Type: application/json" -XPOST 'http://106.75.171.152:9200/score/_doc/1' -d'{"name":"tom","subject":"chinese","score":75}'
 * curl -H "Content-Type: application/json" -XPOST 'http://106.75.171.152:9200/score/_doc/2' -d'{"name":"tom","subject":"math","score":80}'
 * curl -H "Content-Type: application/json" -XPOST 'http://106.75.171.152:9200/score/_doc/3' -d'{"name":"jerry","subject":"chinese","score":74}'
 * curl -H "Content-Type: application/json" -XPOST 'http://106.75.171.152:9200/score/_doc/4' -d'{"name":"jerry","subject":"math","score":81}'
 * curl -H "Content-Type: application/json" -XPOST 'http://106.75.171.152:9200/score/_doc/5' -d'{"name":"dave","subject":"chinese","score":72}'
 * curl -H "Content-Type: application/json" -XPOST 'http://106.75.171.152:9200/score/_doc/6' -d'{"name":"dave","subject":"math","score":75}'
 * @version: 0.0.1
 * @author: liwenhui
 * @createTime: 2022-11-20 19:51
 **/
public class EsAggOp {
    private static final String INDICES_NAME = "score";

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = getRestHighLevelClient();

        SearchRequest searchRequest = new SearchRequest(INDICES_NAME);

        //指定查询条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //指定分组名称，为name_term
        TermsAggregationBuilder aggregation = AggregationBuilders.terms("name_term")
                //指定分组字段，如果是字符串（Text）类型，则需要指定使用keyword类型
                .field("name.keyword")
                //指定求sum，也支持avg、min、max等操作
                .subAggregation(AggregationBuilders.sum("sum_score").field("score"))
                //设置分组数据返回的条数，默认是10条。
                //如果想获取所有分组的数据：Integer.MAX_VALUE 。如果数据太多，会对es造成压力
                .size(1)
                ;
        searchSourceBuilder.aggregation(aggregation);

        //增加分页参数，注意：分页参数针对分组数据是无效的
        //searchSourceBuilder.from(0).size(20);

        searchRequest.source(searchSourceBuilder);
        //执行查询操作
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        //获取分组信息
        Terms terms = searchResponse.getAggregations().get("name_term");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            //获取sum聚合结果
            Sum sum = bucket.getAggregations().get("sum_score");
            System.out.println(bucket.getKey() + "--" + sum.getValue());
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
