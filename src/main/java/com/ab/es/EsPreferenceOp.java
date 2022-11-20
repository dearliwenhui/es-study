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
 * @description: 偏好查询（分片查询方式）
 * @version: 0.0.1
 * @author: liwenhui
 * @createTime: 2022-11-20 21:44
 **/
public class EsPreferenceOp {

    private static final String INDICES_NAME = "pre";

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = getRestHighLevelClient();

        SearchRequest searchRequest = new SearchRequest(INDICES_NAME);
        searchRequest.indices("pre");

        //指定分片查询方式
        //默认是randomize across shards：表示随机从分片中取数据
        //searchRequest.preference();

        //_local：表示查询操作会优先在本地节点(协调节点)的分片中查询，没有的话再到其它节点中查询。
        /**
         * searchRequest.preference("_local");
         * 这种查询方式可以提高性能，在一个node上查询，不需要跨节点网络传输。
         * 但是当某一时刻接收到的查询请求较多时，会对这个节点产生压力
         */
        //searchRequest.preference("_local");

        //_only_local：表示查询只会在本地节点的分片中查询，如果协调节点只有部分分片的数据，那么就只会查询部分分片里面数据。
         //数据虽然快，但是不完整。因为我们没有办法保证索引库的分片，正好都分到这一个节点上。
        //searchRequest.preference("_only_local");

        //_only_nodes：表示只在指定的节点中查询。只在指定的节点中查询某一个索引库的分片里面的信息，
        // 但注意这里指定那个节点列表里面，它必须包含指定索引库的所有分片，如果从这些节点列表中获取到的索引库的分片个数不完整，程序会报错。
        //适用于在某种特殊情况下，集群中的个别节点，压力比较大，短时间无法恢复。那么我们在查询的时候，就可以规避掉这些节点，只选择一些正常的节点进行查询。
        //前提是这个索引库的分片有副本，如果没有副本，这个分片就只有一个主分片。就算这个主分片的节点压力比较大，那也只能查询这个节点了。
        //必须获取到节点id，集群使用","分隔:_only_nodes:nodeId1,nodeId2,nodeId3
        //searchRequest.preference("_only_nodes:jLVYeu9jQ7qPwS3LVgTI3g");

        //_prefer_nodes：表示优先在指定的节点上查询。如果某个节点比较空闲，就可以尽可能的多在这个节点上去做一些查询操作，减轻集群中其他节点的压力，尽可能的实现一个负载均衡。
        //searchRequest.preference("_prefer_nodes:jLVYeu9jQ7qPwS3LVgTI3g");

        //_shards：表示只查询索引库中指定分片的数据。可以指定一个或者多个分片，分片编号从0开始
        //如果我们提前已经知道我们需要查询的数据都在这个索引库的哪些分片里面，那我们在这里面提前指定对应分片的一个编号，这样查询请求就只会到这些分片里面进行查询，提高查询效率，减轻集群压力。
        //searchRequest.preference("_shards:0,1");

        //custom-string：自定义一个参数，不能以下划线"_"开头。
        //有时候希望多次查询使用索引库中相同的分片，因为分片有副本，正常情况下，两次查询使用的分片可能不一样。
        //如果search type使用的是query then fetch ， 此时分片里面的数据在计算打分依据的时候，是根据当前节点里面的词频和文档频率，
        // 两次查询使用的分片不是同一个，这样就会导致在计算打分依据的时候使用的样本不一致，最终导致两次相同的查询条件，返回的结果不一样。
        // dfs query then fetch不会有这个问题，但是性能损耗会大些。
        //searchRequest.preference("abc");

        //最常见的是_shards，因为可以显著提高查询效率

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
