package com.ab.core.utils;

import com.ab.core.Article;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @description:
 * @version: 0.0.1
 * @author: liwenhui
 * @createTime: 2022-11-27 16:44
 **/
public class EsUtil {
    private EsUtil() {
    }

    private static RestHighLevelClient client;

    static {
        //获取RestClient连接
        //注意：高级别客户端其实是对低级别客户端的代码进行了封装，所以连接池使用的是低级别客户端中的连接池
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("myhost", 9200, "http"))
                        /*.setHttpClientConfigCallback(httpAsyncClientBuilder ->
                                httpAsyncClientBuilder.setDefaultIOReactorConfig(
                                        IOReactorConfig.custom()
                                                //设置线程池中线程的数量，默认是客户端机器可用CPU数量
                                                .setIoThreadCount(1)
                                                .build()
                                ))*/);
    }

    /**
     * 获取客户端
     * @return
     */
    public static RestHighLevelClient getClient() {
        return client;
    }

    /**
     * 关闭客户端
     * 注意：调用高级别客户单的close方法时，会将低级别客户端创建的连接池整个关闭掉，最终导致client无法继续使用
     * 所以正常是用不到这个close方法的，只有在程序结束的时候才需要调用
     * 不调用也没有问题，程序结束了连接也断了
     * @throws IOException
     */
    public static void closeRestClient() throws IOException {
        client.close();
    }

    /**
     * 建立索引
     * @param index
     * @param id
     * @param source
     * @throws IOException
     */
    public static void addIndex(String index, String id, Map<String, String> source) throws IOException {
        IndexRequest request = new IndexRequest(index);
        request.id(id);
        request.source(source);
        client.index(request, RequestOptions.DEFAULT);
    }

    public static Map<String, Object> search(String key, String index, int start, int row) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(index)
                .searchType(SearchType.DFS_QUERY_THEN_FETCH);
        //组装查询条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        if (key!=null) {
            searchSourceBuilder.query(QueryBuilders.multiMatchQuery(key, "title", "describe", "content"));
        }
        //分页
        searchSourceBuilder.from(start);
        searchSourceBuilder.size(row);

        //高亮
        HighlightBuilder highlightBuilder = new HighlightBuilder().field("title").field("describe");
        //设置高亮字段的前缀和后缀内容
        highlightBuilder.preTags("<font color='red'>");
        highlightBuilder.postTags("</font>");
        searchSourceBuilder.highlighter(highlightBuilder);

        //指定查询条件
        searchRequest.source(searchSourceBuilder);

        //执行查询操作
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        HashMap<String, Object> resultMap = new HashMap<>();
        SearchHits hits = searchResponse.getHits();
        long totalHitsCount = hits.getTotalHits().value;
        resultMap.put("total", totalHitsCount);
        List<Article> articleList = new ArrayList<>();
        //获取具体内容
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String id = hit.getId();
            String title = sourceAsMap.get("title").toString();
            String author = sourceAsMap.get("author").toString();
            String describe = sourceAsMap.get("describe").toString();
            String time = sourceAsMap.get("time").toString();

            //获取高亮字段内容
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            //获取title字段的高亮内容
            HighlightField titleHighlightField = highlightFields.get("title");
            if (titleHighlightField != null) {
                Text[] fragments = titleHighlightField.getFragments();
                title = "";
                for (Text fragment : fragments) {
                    title += fragment;
                }
            }

            //获取describe字段的高亮内容
            HighlightField describeHighlightField = highlightFields.get("describe");
            if (describeHighlightField != null) {
                Text[] fragments = describeHighlightField.getFragments();
                describe = "";
                for (Text fragment : fragments) {
                    title += fragment;
                }
            }

            Article article = new Article();
            article.setId(id);
            article.setTitle(title);
            article.setAuthor(author);
            article.setDescribe(describe);
            article.setTime(time);
            articleList.add(article);
        }
        resultMap.put("dataList", articleList);
        return resultMap;
    }


    public static void main(String[] args) throws IOException {
        Map<String, Object> search = search("总书记", "article", 0, 10);
        System.out.println(search);
    }

}
