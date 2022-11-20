package com.ab.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Map;

/**
 *
 * search详解
 * @初始化数据：
 * curl -H "Content-Type: application/json" -XPOST 'http://106.75.171.152:9200/user/_doc/1' -d'{"name":"tom","age":25,"nickname":"tt"}'
 * curl -H "Content-Type: application/json" -XPOST 'http://106.75.171.152:9200/user/_doc/2' -d'{"name":"tom","age":30,"nickname":"tt"}'
 * curl -H "Content-Type: application/json" -XPOST 'http://106.75.171.152:9200/user/_doc/3' -d'{"name":"Stephenson","age":34,"nickname":"ss"}'
 * curl -H "Content-Type: application/json" -XPOST 'http://106.75.171.152:9200/user/_doc/4' -d'{"name":"Jones","age":21,"nickname":"tom"}'
 * curl -H "Content-Type: application/json" -XPOST 'http://106.75.171.152:9200/user/_doc/5' -d'{"name":"Mercedes","age":42,"nickname":"mm"}'
 * curl -H "Content-Type: application/json" -XPOST 'http://106.75.171.152:9200/user/_doc/6' -d'{"name":"Hammond","age":23,"nickname":"hh"}'
 * curl -H "Content-Type: application/json" -XPOST 'http://106.75.171.152:9200/user/_doc/7' -d'{"name":"Bennett","age":30,"nickname":"bb"}'
 * curl -H "Content-Type: application/json" -XPOST 'http://106.75.171.152:9200/user/_doc/8' -d'{"name":"Leonard","age":25,"nickname":"ll"}'
 * curl -H "Content-Type: application/json" -XPOST 'http://106.75.171.152:9200/user/_doc/9' -d'{"name":"Green","age":35,"nickname":"gg"}'
 * curl -H "Content-Type: application/json" -XPOST 'http://106.75.171.152:9200/user/_doc/10' -d'{"name":"Dorsey","age":33,"nickname":"dd"}'
 * curl -H "Content-Type: application/json" -XPOST 'http://106.75.171.152:9200/user/_doc/11' -d'{"name":"Jenkins","age":25,"nickname":"jj"}'
 * curl -H "Content-Type: application/json" -XPOST 'http://106.75.171.152:9200/user/_doc/12' -d'{"name":"李逵","age":30,"nickname":"黑旋风"}'
 * curl -H "Content-Type: application/json" -XPOST 'http://106.75.171.152:9200/user/_doc/13' -d'{"name":"李鬼","age":25,"nickname":"黑旋风"}'
 * @使用search查询返回的例子
 *
 * {
 * 	"took": 549,
 * 	"timed_out": false,
 * 	"_shards": {
 * 		"total": 1,
 * 		"successful": 1,
 * 		"skipped": 0,
 * 		"failed": 0
 *        },
 * 	"hits": {
 * 		"total": {
 * 			"value": 2,
 * 			"relation": "eq"
 *        },
 * 		"max_score": 1,
 * 		"hits": [{
 * 				"_index": "emp",
 * 				"_type": "_doc",
 * 				"_id": "i8dxi4QB7jbiPedIzZ8x",
 * 				"_score": 1,
 * 				"_source": {
 * 					"name": "tom",
 * 					"age": 40
 *                }
 *            },
 *            {
 * 				"_index": "emp",
 * 				"_type": "_doc",
 * 				"_id": "20",
 * 				"_score": 1,
 * 				"_source": {
 * 					"field1": "v1",
 * 					"field2": "value2",
 * 					"newField": "newValue"
 *                }
 *            }
 * 		]
 *    }
 * }
 *
 * @version: 0.0.1
 * @author: liwenhui
 * @createTime: 2022-11-19 23:27
 **/
public class EsSearchOp {

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = getRestHighLevelClient();

        SearchRequest searchRequest = new SearchRequest();
        //指定索引库，支持指定一个或者多个，也支持通配符，例如：user*
        searchRequest.indices("user");


        //执行查询条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //查询所有，可以不指定，默认就是查询索引库中的所有数据
//        searchSourceBuilder.query(QueryBuilders.matchAllQuery());

        //对指定字段的值进行过滤，注意：在查询数据的时候会对数据进行分词
        //如果指定多个query，后面的query会覆盖前面的query
        //针对字符串类型内容的查询，不支持通配符
//        searchSourceBuilder.query(QueryBuilders.matchQuery("name", "tom"));
        //针对age的值，可以指定字符串或数值
//        searchSourceBuilder.query(QueryBuilders.matchQuery("age", 30));

        //通配符不是明确的找某个索引，没有办法分词，只能全表扫描索引。
        //例如找出to*，最终找出es存储的索引片段里面有哪些是以to开头的。
        //针对字符串类型内容的查询，支持通配符，但是性能较差，可以认为是全表扫描
//        searchSourceBuilder.query(QueryBuilders.wildcardQuery("name", "to*"));

        //区间查询，主要针对数据类型，可以使用from(>=)+to(<=)或者gt,gte+lt,lte
//        searchSourceBuilder.query(QueryBuilders.rangeQuery("age").from(0).to(30));
        //效果和from,to一样
//        searchSourceBuilder.query(QueryBuilders.rangeQuery("age").gte(0).lte(30));
        //不限制边界，指定为nul1即可，也可以不指定to
//        searchSourceBuilder.query(QueryBuilders.rangeQuery("age").from(0).to(null));

        //同时指定多个条件，条件之间的关系支持and（must）、or（should）
        //查出name=tom || age=30 的记录
//        searchSourceBuilder.query(QueryBuilders.boolQuery()
//                .should(QueryBuilders.matchQuery("name", "tom"))
//                .should(QueryBuilders.matchQuery("age", 30)));

        //多条件组合查询的时候，可以设置条件的权重值，将满足高权重值条件的数据排到结果列表的前面
        //匹配到age的数据排在前面
        /*searchSourceBuilder.query(QueryBuilders.boolQuery()
                .should(QueryBuilders.matchQuery("name", "tom").boost(1.0f))
                .should(QueryBuilders.matchQuery("age", 30).boost(5.0f)));*/

        //对多个指定字段的值进行过滤，注意：多个字段的数据类型必须一致，否则会报错，如果查询的字段不存在不会报错
//        searchSourceBuilder.query(QueryBuilders.multiMatchQuery("tom", "name", "nickname"));


        //使用Lucene语法
        //通过queryStringQuery可以支持Lucene的原生查询语法，更加灵活，注意：AND、OR、To之类的关键字必须大写
//        searchSourceBuilder.query(QueryBuilders.queryStringQuery("name:tom AND age:[28 TO 31]"));
        /*searchSourceBuilder.query(QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("name", "tom"))
                .must(QueryBuilders.rangeQuery("age").from(28).to(31)));*/
        //querystringQuery支持通配符，但是性能也是比较差。通配符必定需要全表扫描
//        searchSourceBuilder.query(QueryBuilders.queryStringQuery("name:t*"));

        //精确查询，查询的时候不分词，针对人名、手机号、主机名、邮箱号码等字段的查询时一般不需要分词
        //默认情况下在建立索引的时候李逵会被切分为李、逵这两个词
        //所以这里精确查询是查不出来的，使用matchQuery可以查出来，因为matchQuery会进行分词。
        //在创建索引库时，针对name字段在建立索引的时候不分词，才能使用精确匹配。
//        searchSourceBuilder.query(QueryBuilders.termQuery("name", "李逵"));
        //正常情况下想要使用termQuery实现精确查询的字段不能进行分词
        //但是有时候会遇到某入字段已经分词建立索引了，后期还想要实现精确查询
        //可以使用queryStringQuery将查询的词用""修饰
        //searchSourceBuilder.query(QueryBuilders.queryStringQuery("name:\"李逵\""));
        //matchQuery默认会根据分词的结果进行 or 操作，满足任意一个词语的数据都会查询出来
        //如果想要对matchQuery的分词结果实现and操作，可以通过operator进行设置
        //这种方式也可以解决某个字段已经分词建立索引了，后期还想要实现精确查询的问题（间接实现，其实是查询了同时满足李、逵这两个词语的内容）
//        searchSourceBuilder.query(QueryBuilders.matchQuery("name", "李逵").operator(Operator.AND));
        //通过keyword也能实现精确查询，可以在matchQuery中指定字段的keyword类型实现精确查询，不管在建立索引的时候有没有被分词都不影响使用
//        searchSourceBuilder.query(QueryBuilders.termQuery("name.keyword", "李逵"));


        //分页
        //设置每页的起始位置默认是0
        searchSourceBuilder.from(0);
        //设置每页的数据量，默认是10
        searchSourceBuilder.size(10);

        //排序
        //注意：age字段是数字类型，不需要分词，name字段是字符串类型（Text），默认会被分词，所以不支持排序和聚合操作
//        searchSourceBuilder.sort("age", SortOrder.DESC);
        //如果想要根据这些会被分词的字段进行排序或者聚合，需要指定使用他们的keyword类型，这个类型表示不会对数据分词
//        searchSourceBuilder.sort("name.keyword", SortOrder.DESC);


        //高亮
        //设置高亮字段
        HighlightBuilder highlightBuilder = new HighlightBuilder()
                .field("name");//支持多个高亮字段，使用多个field方法指定即可
        //设置高亮字段的前缀和后缀内容
        highlightBuilder.preTags("<font color='red'>");
        highlightBuilder.postTags("</font>");
        searchSourceBuilder.highlighter(highlightBuilder);
        //设置好高亮关键字后，在解析的时候还需要获取高亮关键字
        //条件查询name，触发高亮
        searchSourceBuilder.query(QueryBuilders.matchQuery("name", "李逵"));



        searchRequest.source(searchSourceBuilder);

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
        System.out.println("---------------------高亮关键字--------------------------");
        for (SearchHit hit : searchHits) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = sourceAsMap.get("name").toString();
            int age = Integer.parseInt(sourceAsMap.get("age").toString());
            //获取高亮字段
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            //获取name高亮字段
            HighlightField highlightField = highlightFields.get("name");
            if (highlightField != null) {
                Text[] fragments = highlightField.getFragments();
                name = "";
                //fragments可能会有多个高亮关键字，所以需要拼接
                for (Text fragment : fragments) {
                    name += fragment;
                }
            }
            System.out.println(name + "-----" + age);

        }

        close(client);
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
