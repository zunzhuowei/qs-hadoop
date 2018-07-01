package com.qs.game.starter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.alibaba.fastjson.JSON;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequestBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.wltea.analyzer.lucene.IKAnalyzer;


public class TransportClientTest {


    private TransportClient client;


    private final static String article = "article";
    private final static String content = "content";

    @Before
    public void getClient() throws Exception {
        //设置集群名称
        Settings settings = Settings.builder().put("cluster.name", "my-elasticsreach-application")
                .put("client.transport.sniff",true)
                .build();// 集群名

        //创建client
        client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress
                        (InetAddress.getByName("192.168.1.233"), 9300));
    }

    /**
     * -----------------------------------------增(创建索引,增加映射,新增文档)
     */


    /**
     * 创建索引的四种方法
     */

    @Test
    public void JSON() {
        String json = "{" +
                "\"id\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";

    }

    /**
     * 创建索引并添加映射
     *
     * @throws IOException
     */
    @Test
    public void CreateIndexAndMapping() throws Exception {
        AnalyzeRequest request = new AnalyzeRequest();

        CreateIndexRequestBuilder cib = client.admin().indices().prepareCreate(article);
        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties") //设置之定义字段
                .startObject("author")
                .field("type", "string") //设置数据类型
                .endObject()
                .startObject("title")
                .field("type", "string").field("analyzer","ik_max_word")
                .endObject()
                .startObject("content")
                .field("type", "string").field("analyzer","ik_max_word")
                .endObject()
                .startObject("price")
                .field("type", "string")
                .endObject()
                .startObject("view")
                .field("type", "string")
                .endObject()
                .startObject("tag")
                .field("type", "string").field("analyzer","ik_max_word")
                .endObject()
                .startObject("date")
                .field("type", "date")  //设置Date类型
                .field("format", "yyyy-MM-dd HH:mm:ss") //设置Date的格式
                .endObject()
                .endObject()
                .endObject();
        cib.addMapping(content, mapping);

        CreateIndexResponse res = cib.execute().actionGet();

        System.out.println("----------添加映射成功----------");
    }

    /**
     * 创建索引并添加文档
     *
     * @throws Exception
     */
    @Test
    public void addIndexAndDocument() throws Exception {

        Date time = new Date();

        IndexResponse response = client.prepareIndex(article, content)
                .setSource(XContentFactory.jsonBuilder().startObject()
                        .field("id", "447")
                        .field("author", "fendo")
                        .field("title", "192.138.1.2")
                        .field("content", "这是JAVA有关的书籍")
                        .field("price", "20")
                        .field("view", "100")
                        .field("tag", "a,b,c,d,e,f")
                        .field("date", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time))
                        .endObject())
                .get();
        System.out.println("添加索引成功,版本号：" + response.getVersion());
    }


    /**
     * -------------------------------------Bulk---------------------------------
     */


    /**
     * bulkRequest
     *
     * @throws Exception
     */
    @Test
    public void bulkRequest() throws Exception {
        BulkRequestBuilder bulkRequest = client.prepareBulk();

        Date time = new Date();

        // either use client#prepare, or use Requests# to directly build index/delete requests
        bulkRequest.add(client.prepareIndex(article, content, "199")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("id", "199")
                        .field("author", "fendo")
                        .field("title", "BULK")
                        .field("content", "这是BULK有关的书籍")
                        .field("price", "40")
                        .field("view", "300")
                        .field("tag", "a,b,c")
                        .field("date", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time))
                        .endObject()
                )
        );

        bulkRequest.add(client.prepareIndex(article, content, "101")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("id", "101")
                        .field("author", "fendo")
                        .field("title", "ACKSE")
                        .field("content", "这是ACKSE有关的书籍")
                        .field("price", "50")
                        .field("view", "200")
                        .field("tag", "a,b,c")
                        .field("date", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time))
                        .endObject()
                )
        );

        BulkResponse bulkResponse = bulkRequest.get();
        if (bulkResponse.hasFailures()) {
            // process failures by iterating through each bulk response item
            //System.out.println(bulkResponse.getTook());
        }
    }


    /**
     * 设置自动提交文档
     * BulkProcessor
     *
     * @throws Exception
     */
    @Test
    public void autoBulkProcessor() throws Exception {

        BulkProcessor bulkProcessor = BulkProcessor.builder(client,
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId,
                                           BulkRequest request) {
                        //提交前调用

                    }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          BulkResponse response) {
                        //提交结束后调用（无论成功或失败）
                        System.out.println("提交" + response.getItems().length + "个文档，用时" + response.getTookInMillis() + "MS" + (response.hasFailures() ? " 有文档提交失败！" : ""));

                    }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          Throwable failure) {
                        //提交结束且失败时调用
                        System.out.println(" 有文档提交失败！after failure=" + failure);

                    }
                })
                //当请求超过10000个（default=1000）或者总大小超过1GB（default=5MB）时，触发批量提交动作。
                .setBulkActions(10000)//文档数量达到1000时提交
                .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))//总文档体积达到5MB时提交
                .setFlushInterval(TimeValue.timeValueSeconds(5))//每5S提交一次（无论文档数量、体积是否达到阈值）
                .setConcurrentRequests(1)//加1后为可并行的提交请求数，即设为0代表只可1个请求并行，设为1为2个并行
                .setBackoffPolicy(
                        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();

        //提交单个
        //String json = "{\"id\":\"66\",\"author\":\"ckse\",\"title\":\"windows编程\",\"content\":\"windows 32 API编程\",\"price\":\"99\",\"view\":\"222\",\"date\":\"2017-08-01 17:21:18\"}";
        //bulkProcessor.add(new IndexRequest("设置的index name", "设置的type name","要插入的文档的ID").source(json));//添加文档，以便自动提交
        for (int i = 0; i < 80080; i++) {
            //业务对象
            String json = "{\"id\":\"" + i + "\",\"author\":\"ckse\",\"title\":\"windows编程\",\"content\":\"windows 32 API编程\",\"price\":\"99\",\"view\":\"222\",\"date\":\"2017-08-01 17:21:18\"}";
            System.out.println(json);
            bulkProcessor.add(new IndexRequest("article", "content", "" + i).source(json));//添加文档，以便自动提交

        }

        System.out.println("创建成功!!!");

    }

    //手动 批量更新
    @Test
    public void multipleBulkProcessor() throws Exception {

        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (int i = 500; i < 1000; i++) {
            //业务对象
            String jsons = "{\"id\":\"" + i + "\",\"author\":\"ckse\",\"title\":\"windows编程\",\"content\":\"windows 32 API编程\",\"price\":\"99\",\"view\":\"222\",\"date\":\"2017-08-01 17:21:18\"}";
            IndexRequestBuilder indexRequest = client.prepareIndex("article", "content")
                    //指定不重复的ID
                    .setSource(jsons).setId(String.valueOf(i));
            //添加到builder中
            bulkRequest.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
            // process failures by iterating through each bulk response item
            System.out.println(bulkResponse.buildFailureMessage());
        }
        System.out.println("创建成功!!!");
    }

    /**
     * 使用Bulk批量添加导入数据
     */
    @Test
    public void ImportBulk() {
        FileReader fr = null;
        BufferedReader bfr = null;
        String line = null;
        try {
            File file = new File("F:\\Source\\Elasticsearch\\TransportClient\\src\\main\\resources\\bulk.txt");
            fr = new FileReader(file);
            bfr = new BufferedReader(fr);
            BulkRequestBuilder bulkRequest = client.prepareBulk();
            int count = 0;
            while ((line = bfr.readLine()) != null) {
                bulkRequest.add(client.prepareIndex(article, content).setSource(line));
                if (count % 10 == 0) {
                    bulkRequest.execute().actionGet();
                }
                count++;
            }
            bulkRequest.execute().actionGet();
            System.out.println("导入成功!!!");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                bfr.close();
                fr.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * 使用Bulk批量导出数据
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void ExportBulk() throws Exception {


        QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
        SearchResponse response = client.prepareSearch("article").setQuery(queryBuilder).get();

        SearchHits resultHits = response.getHits();

        System.out.println(JSON.toJSON(resultHits));

        FileWriter fw = null;
        BufferedWriter bfw = null;
        try {
            File file = new File("F:\\Source\\Elasticsearch\\TransportClient\\src\\main\\resources\\bulk.txt");
            fw = new FileWriter(article);
            bfw = new BufferedWriter(fw);

            if (resultHits.getHits().length == 0) {
                System.out.println("查到0条数据!");

            } else {
                for (int i = 0; i < resultHits.getHits().length; i++) {
                    String jsonStr = resultHits.getHits()[i]
                            .getSourceAsString();
                    System.out.println(jsonStr);
                    bfw.write(jsonStr);
                    bfw.write("\n");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                bfw.close();
                fw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * -----------------------------------------删(删除索引,删除文档)
     */


    /**
     * 删除整个索引库
     */
    @Test
    public void deleteAllIndex() {

        String indexName = "article";

        /**
         * 两种方式如下:
         */

        //1)
        //可以根据DeleteIndexResponse对象的isAcknowledged()方法判断删除是否成功,返回值为boolean类型.
        DeleteIndexResponse dResponse = client.admin().indices().prepareDelete(indexName)
                .execute().actionGet();
        System.out.println("是否删除成功:" + dResponse.isAcknowledged());


        //2)
        //如果传人的indexName不存在会出现异常.可以先判断索引是否存在：
        IndicesExistsRequest inExistsRequest = new IndicesExistsRequest(indexName);

        IndicesExistsResponse inExistsResponse = client.admin().indices()
                .exists(inExistsRequest).actionGet();

        //根据IndicesExistsResponse对象的isExists()方法的boolean返回值可以判断索引库是否存在.
        System.out.println("是否删除成功:" + inExistsResponse.isExists());
    }


    /**
     * 通过ID删除
     */
    @Test
    public void deleteById() {
        DeleteResponse dResponse = client.prepareDelete(article, content, "AV49wyfCWmWw7AxKFxeb").execute().actionGet();
        if ("OK".equals(dResponse.status())) {
            System.out.println("删除成功");
        } else {
            System.out.println("删除失败");
        }
    }


    /**
     * 通过Query delete删除
     */
    @Test
    public void queryDelete() {
         String guid="AV49wyfCWmWw7AxKFxeb";
         String author="kkkkk";
         DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
               .source(article)
               .filter(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("id", guid)).must(QueryBuilders.termQuery("author", author)).must(QueryBuilders.typeQuery(content)))
               .get();
    }


    /**
     * 使用matchAllQuery删除所有文档
     */
    @Test
    public void deleteAll() {
         DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
           .source(article)
           .filter(QueryBuilders.matchAllQuery())
           .get();
    }


    /**
     * bulk批量通过指定id删除方法
     */
    @Test
    public void batchUndercarriageFamilies() {
        List<String> publishIds = new ArrayList<>();
        publishIds.add("AV49wyfCWmWw7AxKFxeY");
        publishIds.add("AV49wyfCWmWw7AxKFxea");
        BulkRequestBuilder builder = client.prepareBulk();
        for (String publishId : publishIds) {
            System.out.println(publishId);
            builder.add(client.prepareDelete(article, content, publishId).request());

        }
        BulkResponse bulkResponse = builder.get();
        System.out.println(bulkResponse.hasFailures());
    }


    /**
     * -----------------------------------------改()
     */


    /**
     * 更新文档
     *
     * @throws Exception
     */
    @Test
    public void updateDocument() throws Exception {

        Date time = new Date();

        //创建修改请求
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(article);
        updateRequest.type(content);
        updateRequest.id("AV4xv5gAZLX8AvCc6ZWZ");
        updateRequest.doc(XContentFactory.jsonBuilder()
                .startObject()
                .field("author", "FKSE")
                .field("title", "JAVA思想")
                .field("content", "注意:这是JAVA有关的书籍")
                .field("date", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time))
                .endObject());

        UpdateResponse response = client.update(updateRequest).get();
        System.out.println("更新索引成功");
    }


    /**
     * -----------------------------有问题：要引入:reindex
     */


    /**
     * UpdateByQueryRequestBuilder
     *
     * @throws Exception
     */
    @Test
    public void updateByQueryRequestBuilder() throws Exception {
        UpdateByQueryRequestBuilder updateByQueryRequestBuilder = UpdateByQueryAction.INSTANCE.newRequestBuilder(client);
        updateByQueryRequestBuilder
                .script(new Script(ScriptType.INLINE,"painless","ctx_source.likes++",null))
                .source()
                .setQuery(QueryBuilders.termQuery("author","kkkkk"))
                .setIndices(article)
                .get();
    }


    /**
     * updateByQueryRequestBuilders
     */
    @Test
    public void updateByQueryRequestBuilders() {

//      Map<String, Object> maps=new HashMap<>();
//      maps.put("orgin_session_id", 10);
//      maps.put("orgin_session_id", 11);
//      maps.put("orgin_session_id", 12);
//      maps.put("orgin_session_id", 13);
//
//      Set<Map<String, Object>> docs = new HashSet<>();
//      docs.add(maps);
//
//      UpdateByQueryRequestBuilder  ubqrb = UpdateByQueryAction.INSTANCE.newRequestBuilder(client);
//        for (Map<String, Object> doc : docs) {
//            if (doc==null || doc.isEmpty()){
//                return;
//            }
//            Script script = new Script("ctx._source.price = ctx._version");
//
//            System.out.println(doc.get("orgin_session_id"));
//
//            //BulkIndexByScrollResponse
//            BulkByScrollResponse scrollResponse = ubqrb.source(article).script(script)
//                            .filter(QueryBuilders.matchAllQuery()).get();
//            for (BulkItemResponse.Failure failure : scrollResponse.getBulkFailures()) {
//              System.out.println(failure.getMessage());
//            }
//        }
    }

    /**
     * prepareUpdate
     *
     * @throws Exception
     */
    @Test
    public void prepareUpdate() throws Exception {

        XContentBuilder endObject = XContentFactory.jsonBuilder().startObject().field("author", "AAAAAAAAAAAAAAA").endObject();
        UpdateResponse response = client.prepareUpdate(article, content, "AV49wyfCWmWw7AxKFxeb").setDoc(endObject).get();
        System.out.println(response.getVersion());

    }


    /**
     * -----------------------------------------查()
     */


    /**
     * 根据index、type、id进行查询
     */
    @Test
    public void searchByIndex() {

        GetResponse response = client.prepareGet(article, content, "AV49wyfCWmWw7AxKFxec").execute()
                .actionGet();
        String json = response.getSourceAsString();
        if (null != json) {
            System.out.println(json);
        } else {
            System.out.println("未查询到任何结果！");
        }
    }


    /**
     * 查询article索引下的所有数据
     * <a href='https://www.elastic.co/guide/en/elasticsearch/client/java-api/5.5/java-query-dsl-match-all-query.html'>
     *
     * @throws Exception
     */
    @Test
    public void matchAllQuery() throws Exception {
        QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
        SearchResponse response = client.prepareSearch(article).setQuery(queryBuilder).get();
        for (SearchHit searchHit : response.getHits()) {
            println(searchHit);
        }
    }


    /**
     * 查询article索引下的articledate的所有数据
     *
     * @throws Exception
     */
    @Test
    public void searchmethod1() throws Exception {
        SearchResponse response = client.prepareSearch(article).setTypes(content).get();
        println(response);
        for (SearchHit searchHit : response.getHits()) {
            println(searchHit);
        }
    }

    /**
     * spanFirstQuery
     */
    @Test
    public void spanFirstQuery() {

        // Span First
        QueryBuilder queryBuilder = QueryBuilders.spanFirstQuery(
                QueryBuilders.spanTermQuery("title", "C"),  // Query
                30000                                             // Max查询范围的结束位置
        );
        SearchResponse response = client.prepareSearch(article).setQuery(queryBuilder).get();
        for (SearchHit searchHit : response.getHits()) {
            println(searchHit);
        }
    }


    /**
     * spanNearQuery
     */
    @Test
    public void spanNearQuery() {

        QueryBuilder queryBuilder = QueryBuilders.spanNearQuery(QueryBuilders.spanTermQuery("title", "C"), 1000)
                .addClause(QueryBuilders.spanTermQuery("name", "葫芦580娃")) // Span Term Queries
                .addClause(QueryBuilders.spanTermQuery("name", "葫芦3812娃"))
                .addClause(QueryBuilders.spanTermQuery("name", "葫芦7139娃"));
        SearchResponse response = client.prepareSearch(article).setQuery(queryBuilder).get();
        for (SearchHit searchHit : response.getHits()) {
            println(searchHit);
        }
    }

    /**
     * spanNotQuery
     */
    @Test
    public void spanNotQuery() {
        QueryBuilder queryBuilder = QueryBuilders.spanNotQuery(QueryBuilders.spanTermQuery("title", "C"), null);
        SearchResponse response = client.prepareSearch(article).setQuery(queryBuilder).get();
        for (SearchHit searchHit : response.getHits()) {
            println(searchHit);
        }
    }

    /**
     * spanOrQuery
     */
    @Test
    public void spanOrQuery() {
        QueryBuilder queryBuilder = QueryBuilders.spanOrQuery(QueryBuilders.spanTermQuery("title", "C"));
        SearchResponse response = client.prepareSearch(article).setQuery(queryBuilder).get();
        for (SearchHit searchHit : response.getHits()) {
            println(searchHit);
        }
    }


    public void moreLikeThisQuery() {

    }

    /**
     * 指定单查询条件
     * <a href='https://www.elastic.co/guide/en/elasticsearch/client/java-api/5.5/java-full-text-queries.html#java-query-dsl-simple-query-string-query'>
     *
     * @throws Exception
     */
    @Test
    public void matchQuery() throws Exception {
        QueryBuilder qb = QueryBuilders.matchQuery(
                "title",
                "C"
        );
        SearchResponse response = client.prepareSearch(article).setQuery(qb).get();
        for (SearchHit searchHit : response.getHits()) {
            println(searchHit);
        }
    }


    /**
     * termQuery 查询
     *
     * @throws Exception
     */
    @Test
    public void termQuery() throws Exception {

        QueryBuilder queryBuilder = QueryBuilders.termQuery("id", "11");
        SearchResponse response = client.prepareSearch(article).setQuery(queryBuilder).get();
        for (SearchHit searchHit : response.getHits()) {
            println(searchHit);
        }
    }

    /**
     * termQuery 查询
     * <a href='https://www.elastic.co/guide/en/elasticsearch/client/java-api/5.5/java-term-level-queries.html'>
     *
     * @throws Exception
     */
    @Test
    public void termsQuerys() throws Exception {
        QueryBuilder queryBuilder = QueryBuilders.termsQuery("id", "1", "2");
        SearchResponse response = client.prepareSearch("article").setQuery(queryBuilder).get();
        for (SearchHit searchHit : response.getHits()) {
            println(searchHit);
        }
    }

    /**
     * 范围查询RangeQuery
     * <a href='https://www.elastic.co/guide/en/elasticsearch/client/java-api/5.5/java-term-level-queries.html'>
     *
     * @throws Exception
     */
    @Test
    public void rangeQuery() throws Exception {
        QueryBuilder queryBuilder = QueryBuilders.rangeQuery("price")
                .from(1)
                .to(100)
                .includeLower(false)
                .includeUpper(false);
        // A simplified form using gte, gt, lt or lte
        QueryBuilder _qb = QueryBuilders.rangeQuery("price")
                .gte("10")
                .lt("20");
        SearchResponse response = client.prepareSearch(article).setQuery(queryBuilder).get();
        for (SearchHit searchHit : response.getHits()) {
            println(searchHit);
        }
    }


    /**
     * multiMatchQuery 查询
     * multiMatchQuery针对的是多个field,当fieldNames有多个参数时，如field1和field2，那查询的结果中，要么field1中包含text，要么field2中包含text。
     */
    @Test
    public void multiMatchQuery() {
        QueryBuilder queryBuilder = QueryBuilders.multiMatchQuery("JAVA编程思想", "title", "content");
        SearchResponse response = client.prepareSearch(article).setQuery(queryBuilder).get();
        for (SearchHit searchHit : response.getHits()) {
            println(searchHit);
        }
    }

    /**
     * MultiMatchQueryBuilder
     * <a href='https://www.elastic.co/guide/en/elasticsearch/client/java-api/5.5/java-full-text-queries.html#java-query-dsl-simple-query-string-query'>
     *
     * @throws Exception
     */
    @Test
    public void MultiMatchQueryBuilder() throws Exception {
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery("JAVA编程思想", "title", "content");

        multiMatchQueryBuilder.analyzer("standard");
        multiMatchQueryBuilder.cutoffFrequency(0.001f);
        multiMatchQueryBuilder.field("title", 20);
        multiMatchQueryBuilder.fuzziness(Fuzziness.TWO);
        multiMatchQueryBuilder.maxExpansions(100);
        multiMatchQueryBuilder.prefixLength(10);
        multiMatchQueryBuilder.tieBreaker(20);
        multiMatchQueryBuilder.type(MultiMatchQueryBuilder.Type.BEST_FIELDS);
        multiMatchQueryBuilder.boost(20);


        SearchResponse searchResponse = client.prepareSearch()
                .setIndices(article)
                .setTypes(content)
                .setQuery(multiMatchQueryBuilder)
                .execute()
                .actionGet();

        for (SearchHit searchHit : searchResponse.getHits()) {
            println(searchHit);
        }
    }


    /**
     * MatchQueryBuilder
     *
     * @throws Exception
     */
    @Test
    public void MatchQueryBuilder() throws Exception {

        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("title", "JAVA编程思想");
        matchQueryBuilder.fuzziness(Fuzziness.AUTO);
        SearchResponse searchResponse = client.prepareSearch()
                .setIndices(article)
                .setTypes(content)
                .setQuery(matchQueryBuilder)
                .execute()
                .actionGet();
        println(searchResponse);
    }


    /**
     * 和matchQuery一样
     * <a href='https://www.elastic.co/guide/en/elasticsearch/client/java-api/5.5/java-full-text-queries.html#java-query-dsl-simple-query-string-query'>
     *
     * @throws Exception
     */
    @Test
    public void commonTermsQuery() throws Exception {
        QueryBuilder queryBuilder = QueryBuilders.commonTermsQuery("id",
                "1");
        SearchResponse response = client.prepareSearch(article).setQuery(queryBuilder).get();
        for (SearchHit searchHit : response.getHits()) {
            println(searchHit);
        }
    }


    /**
     * MultiGetResponse  查询多个xxx的值
     * <a href='https://www.elastic.co/guide/en/elasticsearch/client/java-api/5.5/java-docs-multi-get.html'>
     *
     * @throws Exception
     */
    @Test
    public void MultiGetResponse() throws Exception {
        MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
                .add(article, content, "526")
                .add(article, content, "572", "582", "613")
                .get();
        for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
            GetResponse response = itemResponse.getResponse();
            if (response.isExists()) {
                String json = response.getSourceAsString();
                System.out.println(json);
            }
        }
    }

    /**
     * +包含 -除外
     * <a href='https://www.elastic.co/guide/en/elasticsearch/client/java-api/5.5/java-full-text-queries.html#java-query-dsl-simple-query-string-query'>
     *
     * @throws Exception
     */
    @Test
    public void queryStringQuery() throws Exception {
        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("*:*");
        SearchResponse response = client.prepareSearch(article).setQuery(queryBuilder).get();
        for (SearchHit searchHit : response.getHits()) {
            println(searchHit);
        }
    }

    /**
     * +包含 -除外
     * <a href='https://www.elastic.co/guide/en/elasticsearch/client/java-api/5.5/java-full-text-queries.html#java-query-dsl-simple-query-string-query'>
     *
     * @throws Exception
     */
    @Test
    public void simpleQueryStringQuery() throws Exception {
        QueryBuilder queryBuilder = QueryBuilders.simpleQueryStringQuery("+id:1");
        SearchResponse response = client.prepareSearch(article).setQuery(queryBuilder).get();
        for (SearchHit searchHit : response.getHits()) {
            println(searchHit);
        }
    }


    /**
     * existsQuery
     * 匹配含有id字段的记录
     * <a href='https://www.elastic.co/guide/en/elasticsearch/client/java-api/5.5/java-term-level-queries.html'>
     *
     * @throws Exception
     */
    @Test
    public void existsQuery() throws Exception {
        QueryBuilder queryBuilder = QueryBuilders.existsQuery("id");
        SearchResponse response = client.prepareSearch(article).setQuery(queryBuilder).get();
        for (SearchHit searchHit : response.getHits()) {
            println(searchHit);
        }
    }


    /**
     * prefixQuery  匹配包含具有指定前缀的术语的文档的查询
     * 匹配title中前缀为JAVA的记录
     * 匹配分词前缀 如果字段没分词，就匹配整个字段前缀
     * 前缀匹配（比如我要查询的是192.168.1.12，但是当输入192.168、192.168.1、192.168.1.1等的情况都会有相应结果返回，只不过是个范围)
     * <a href='https://www.elastic.co/guide/en/elasticsearch/client/java-api/5.5/java-term-level-queries.html'>
     *
     * @throws Exception
     */
    @Test
    public void prefixQuery() throws Exception {
        QueryBuilder queryBuilder = QueryBuilders.prefixQuery(
                "title",
                "192.138"
        );
        SearchResponse response = client.prepareSearch(article).setQuery(queryBuilder).get();
        for (SearchHit searchHit : response.getHits()) {
            println(searchHit);
        }
    }


    /**
     * MatchPhrasePrefixQueryBuilder  为提供的字段名称和文本创建一个类型为“PHRASE_PREFIX”的匹配查询。
     *
     * @throws Exception
     */
    @Test
    public void MatchPhrasePrefixQueryBuilder() throws Exception {
        String key = "C++";
        MatchPhrasePrefixQueryBuilder matchPhrasePrefixQueryBuilder = QueryBuilders.matchPhrasePrefixQuery("title", key);

        matchPhrasePrefixQueryBuilder.boost(10);
        matchPhrasePrefixQueryBuilder.analyzer("standard");
        matchPhrasePrefixQueryBuilder.slop(2);
        matchPhrasePrefixQueryBuilder.maxExpansions(100);

        SearchResponse searchResponse = client.prepareSearch()
                .setIndices(article)
                .setTypes(content)
                .setQuery(matchPhrasePrefixQueryBuilder)
                .execute()
                .actionGet();
        for (SearchHit searchHit : searchResponse.getHits()) {
            println(searchHit);
        }
    }


    /**
     * wildcardQuery
     * 通配符
     * <a href='https://www.elastic.co/guide/en/elasticsearch/client/java-api/5.5/java-term-level-queries.html'>
     *
     * @throws Exception
     */
    @Test
    public void wildcardQuery() throws Exception {
        QueryBuilder queryBuilder = QueryBuilders.wildcardQuery("author", "*e");//J?V*
        SearchResponse response = client.prepareSearch(article).setQuery(queryBuilder).get();
        for (SearchHit searchHit : response.getHits()) {
            println(searchHit);
        }
    }

    /**
     * fuzzyQuery  使用模糊查询匹配文档的查询
     *
     * @throws Exception
     */
    @Test
    public void fuzzyQuery() throws Exception {
        QueryBuilder queryBuilder = QueryBuilders.fuzzyQuery(
                "author",
                "e"
        );
        SearchResponse response = client.prepareSearch(article).setQuery(queryBuilder).get();
        for (SearchHit searchHit : response.getHits()) {
            println(searchHit);
        }
    }


    /**
     * boolQuery 匹配与其他查询的布尔组合匹配的文档的查询。
     * <a href='https://www.elastic.co/guide/en/elasticsearch/client/java-api/5.5/java-compound-queries.html'>
     *
     * @throws Exception
     */
    @Test
    public void BoostQuery() throws Exception {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("author", "eeee")).boost(100) //设置此查询的权重。 匹配此查询的文件（除正常权重之外）的得分乘以提供的提升。
                .should(QueryBuilders.termQuery("id", "AV5NF_Dbhqf-jFOFkksT").boost(1));

        SearchResponse response = client.prepareSearch("article").setQuery(boolQueryBuilder).execute().get();
        for (SearchHit searchHit : response.getHits()) {
            println(searchHit);
        }

    }


    /**
     * boolQuery
     *
     * @throws Exception
     */
    @Test
    public void boolQuery() throws Exception {
        QueryBuilder qb = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("author", "eeee"))
                .must(QueryBuilders.termQuery("title", "JAVA思想"))
                .mustNot(QueryBuilders.termQuery("content", "C++")) //添加不得出现在匹配文档中的查询。
                .should(QueryBuilders.termQuery("id", "AV5NF_Dbhqf-jFOFkksT"))//添加应该与返回的文档匹配的子句。 对于具有no的布尔查询,子句必须一个或多个SHOULD子句且必须与文档匹配,用于布尔值查询匹配。 不允许null值。
                .filter(QueryBuilders.termQuery("price", "30.3"));//添加一个查询，必须出现在匹配的文档中，但会不贡献得分。 不允许null值。
        SearchResponse response = client.prepareSearch().setQuery(qb).execute().actionGet();

        for (SearchHit searchHit : response.getHits()) {
            println(searchHit);
        }
    }


    /**
     * boostingQuery
     * <a href='https://www.elastic.co/guide/en/elasticsearch/client/java-api/5.5/java-compound-queries.html'>
     *
     * @throws Exception
     */
    @Test
    public void boostingQuery() throws Exception {
        QueryBuilder qb = QueryBuilders.boostingQuery(
                QueryBuilders.termQuery("id", "AV5NF_Dbhqf-jFOFkksR"),
                QueryBuilders.termQuery("title", "C"))
                .negativeBoost(0.2f);//设置负增强因子。
        SearchResponse response = client.prepareSearch().setQuery(qb).execute().actionGet();

        for (SearchHit searchHit : response.getHits()) {
            println(searchHit);
        }
    }


    /**
     * constantScoreQuery
     * <a href='https://www.elastic.co/guide/en/elasticsearch/client/java-api/5.5/java-compound-queries.html'>
     *
     * @throws Exception
     */
    @Test
    public void constantScoreQuery() throws Exception {
        QueryBuilder qb = QueryBuilders.constantScoreQuery(
                QueryBuilders.termQuery("title", "C")
        ).boost(2.0f);
        SearchResponse response = client.prepareSearch().setQuery(qb).execute().actionGet();
        for (SearchHit searchHit : response.getHits()) {
            println(searchHit);
        }
    }

    /**
     * disMaxQuery
     * <a href='https://www.elastic.co/guide/en/elasticsearch/client/java-api/5.5/java-compound-queries.html'>
     *
     * @throws Exception
     */
    @Test
    public void disMaxQuery() throws Exception {
        QueryBuilder qb = QueryBuilders.disMaxQuery()
                .add(QueryBuilders.termQuery("id", "512"))
                .add(QueryBuilders.termQuery("author", "ckse"))
                .boost(1.2f)
                .tieBreaker(0.7f);
        SearchResponse response = client.prepareSearch().setQuery(qb).execute().actionGet();
        for (SearchHit searchHit : response.getHits()) {
            println(searchHit);
        }
    }


    /**
     * functionScoreQuery
     * <a href='https://www.elastic.co/guide/en/elasticsearch/client/java-api/5.5/java-compound-queries.html'>
     *
     * @throws Exception
     */
    @Test
    public void functionScoreQuery() throws Exception {
        FunctionScoreQueryBuilder.FilterFunctionBuilder[] functions = {
                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                        QueryBuilders.matchQuery("id", "512"),
                        ScoreFunctionBuilders.randomFunction("ABCDEF")),
                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                        ScoreFunctionBuilders.exponentialDecayFunction("age", 0L, 1L))
        };
        QueryBuilder qb = QueryBuilders.functionScoreQuery(functions);
        SearchResponse response = client.prepareSearch().setQuery(qb).execute().actionGet();
        for (SearchHit searchHit : response.getHits()) {
            println(searchHit);
        }
    }


    /**
     * regexpQuery 匹配包含具有指定正则表达式的术语的文档的查询。
     * <a href='https://www.elastic.co/guide/en/elasticsearch/client/java-api/5.5/java-term-level-queries.html'>
     *
     * @throws Exception
     */
    @Test
    public void regexpQuery() throws Exception {
        QueryBuilder qb = QueryBuilders.regexpQuery(
                "title",
                "*J");
        SearchResponse response = client.prepareSearch().setQuery(qb).execute().actionGet();
        for (SearchHit searchHit : response.getHits()) {
            println(searchHit);
        }
    }

    /**
     * typeQuery
     * <a href='https://www.elastic.co/guide/en/elasticsearch/client/java-api/5.5/java-term-level-queries.html'>
     *
     * @throws Exception
     */
    @Test
    public void typeQuery() throws Exception {
        QueryBuilder queryBuilder = QueryBuilders.typeQuery("data");
        SearchResponse response = client.prepareSearch(article).setQuery(queryBuilder).get();
        for (SearchHit searchHit : response.getHits()) {
            println(searchHit);
        }
    }

    /**
     * idsQuery
     * 类型是可选的
     * 指定type和id进行查询。
     * <a href='https://www.elastic.co/guide/en/elasticsearch/client/java-api/5.5/java-term-level-queries.html'>
     *
     * @throws Exception
     */
    @Test
    public void idsQuery() throws Exception {
        QueryBuilder queryBuilder = QueryBuilders.idsQuery(content)
                .addIds("512", "520", "531");
        SearchResponse response = client.prepareSearch(article).setQuery(queryBuilder).get();
        for (SearchHit searchHit : response.getHits()) {
            println(searchHit);
        }
    }

    /**
     * group 分组查询
     */
    @Test
    public void group() {


    }

    /**
     * Aggregation
     */
    @Test
    public void Aggregation() {

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        //添加时间范围过滤
        boolQueryBuilder.must(QueryBuilders.rangeQuery("@timestamp").format("yyyy-MM-dd HH:mm:ss").gte("").lte(""));
        AggregationBuilder aggregationBuilder = AggregationBuilders
                //terms(查询字段别名).field(分组字段)
                .terms("").field("")
                .order(Terms.Order.aggregation("", false))
                .size(10)
                .subAggregation(AggregationBuilders.count("").field(""));
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("article").setTypes("articledate")
                .setQuery(boolQueryBuilder)
                .addAggregation(aggregationBuilder)
                .setSize(0);

        SearchResponse sr = searchRequestBuilder.execute().actionGet();
        Terms genders = sr.getAggregations().get("");//统计字段别名
        for (Terms.Bucket entry : genders.getBuckets()) {
            System.out.println((String) entry.getKey() + "-(" + entry.getDocCount() + ")");
        }


        //如想group by 时间，并且按天来进行分组
        AggregationBuilder aggregation = AggregationBuilders
                .dateHistogram("agg")
                .field("@timestamp")
                .format("yyyy-MM-dd")
                .dateHistogramInterval(DateHistogramInterval.DAY);
        //可能有新需求，group by 时间，姓名
        //AggregationBuilder nameAgg = AggregationBuilders.terms(姓名别名).field(姓名).size(10);
        //aggregation.subAggregation(nameAgg);

        //可以能需要进行名称统计，但是需要distinct
        //aggregation.subAggregation(AggregationBuilders.cardinality(别名).field(姓名))

        //其他如下
//        （1）统计某个字段的数量
//        ValueCountBuilder vcb=  AggregationBuilders.count("count_uid").field("uid");
//      （2）去重统计某个字段的数量（有少量误差）
//       CardinalityBuilder cb= AggregationBuilders.cardinality("distinct_count_uid").field("uid");
//      （3）聚合过滤
//      FilterAggregationBuilder fab= AggregationBuilders.filter("uid_filter").filter(QueryBuilders.queryStringQuery("uid:001"));
//      （4）按某个字段分组
//      TermsBuilder tb=  AggregationBuilders.terms("group_name").field("name");
//      （5）求和
//      SumBuilder  sumBuilder= AggregationBuilders.sum("sum_price").field("price");
//      （6）求平均
//      AvgBuilder ab= AggregationBuilders.avg("avg_price").field("price");
//      （7）求最大值
//      MaxBuilder mb= AggregationBuilders.max("max_price").field("price");
//      （8）求最小值
//      MinBuilder min= AggregationBuilders.min("min_price").field("price");
//      （9）按日期间隔分组
//      DateHistogramBuilder dhb= AggregationBuilders.dateHistogram("dh").field("date");
//      （10）获取聚合里面的结果
//      TopHitsBuilder thb=  AggregationBuilders.topHits("top_result");
//      （11）嵌套的聚合
//      NestedBuilder nb= AggregationBuilders.nested("negsted_path").path("quests");
//      （12）反转嵌套
//      AggregationBuilders.reverseNested("res_negsted").path("kps ");


    }


    /**
     * MultiSearchResponse 多字段检索
     */
    @Test
    public void MultiSearchResponse() {


        SearchRequestBuilder srb1 = client.prepareSearch().setQuery(QueryBuilders.queryStringQuery("JAVA"));
        SearchRequestBuilder srb2 = client.prepareSearch().setQuery(QueryBuilders.matchQuery("title", "C"));

        MultiSearchResponse sr = client.prepareMultiSearch().add(srb1).add(srb2).get();

        for (MultiSearchResponse.Item item : sr.getResponses()) {
            SearchResponse response = item.getResponse();
            for (SearchHit searchHit : response.getHits()) {
                println(searchHit);
            }
        }
    }


    /**
     * 复杂查询
     */
    @Test
    public void complexSearch1() {
        int page = 1;
        int pageSize = 10;
        String keyword = "";

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        if (keyword != null && !keyword.equals("")) {
            QueryBuilder nameBuilder = QueryBuilders.matchQuery("zuName", keyword).analyzer("ik_max_word").boost(10);
            QueryBuilder labelBuilder = QueryBuilders.matchQuery("zuLabelName", keyword).analyzer("ik_max_word").boost(10);
            QueryBuilder categoryBuilder = QueryBuilders.matchQuery("categoryName", keyword).analyzer("ik_max_word").boost(10);
            boolQueryBuilder.should(nameBuilder).should(labelBuilder).should(categoryBuilder);
        } else {
            boolQueryBuilder.must(QueryBuilders.matchAllQuery());
        }
        SearchResponse response = client.prepareSearch("article").setTypes("articledate")
                .setQuery(boolQueryBuilder)
                .setFrom((page - 1) * pageSize).setSize(pageSize)
                .setExplain(true)
                .get();

        SearchHits hits = response.getHits();
    }

    /**
     * 复杂查询2
     */
    @Test
    public void complexSearch2() {

        String relatedValue = "fendo";
        String userId = "1234";
        int page = 1;
        int pageSize = 10;

        BoolQueryBuilder builders = new BoolQueryBuilder();
        //加上条件
        builders.must(QueryBuilders.termQuery("userId", userId));
        if (relatedValue == "fendo") {
            builders.must(QueryBuilders.nestedQuery("related4ZuValue",
                    QueryBuilders.boolQuery()
                            .must(QueryBuilders.termQuery("related4ZuValue.nameValue", ""))
                    //.must(QueryBuilders.rangeQuery("endTime").lte(LongformStringDate(System.currentTimeMillis())))
                    , ScoreMode.None));
        } else {
            builders.must(QueryBuilders.nestedQuery("related4ZuValue", QueryBuilders.termQuery("related4ZuValue.nameValue", ""),
                    ScoreMode.None));
        }
        SearchResponse response = client.prepareSearch("article").setTypes("articledate")
                .setQuery(builders).setFrom((page - 1) * pageSize)
                .setSize(pageSize)
                .get();
        SearchHits hits = response.getHits();
    }


    /**
     * 取查询结果总和count
     */
    @Test
    public void countSum() {

        int relatedValue = 1;
        String userId = "111";
        BoolQueryBuilder builders = new BoolQueryBuilder();
        builders.must(QueryBuilders.termQuery("userId", userId));
        if (relatedValue == 1) {
            builders.must(QueryBuilders.nestedQuery("related4ZuValue", QueryBuilders.boolQuery()
                            .must(QueryBuilders.termQuery("related4ZuValue.nameValue", "123"))
                            .must(QueryBuilders.rangeQuery("endTime").lte(""))
                    , ScoreMode.None));

        } else {
            builders.must(QueryBuilders.nestedQuery("related4ZuValue", QueryBuilders.termQuery("related4ZuValue.nameValue", "111"),
                    ScoreMode.None));
        }
        SearchResponse response = client.prepareSearch("article").setTypes("articledate")
                .setQuery(builders)
                .setSize(1)
                .get();
        SearchHits hits = response.getHits();
        System.out.println(hits.getTotalHits());
    }


    /**
     * 聚合求和sum
     */
    @Test
    public void getPlatformZuOrdersTotalAmount() {

        String keyword = "";
        String startTime = "";
        String endTime = "";

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        if (keyword == null || keyword.equals("")) {
            QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
            boolQueryBuilder.must(queryBuilder);
        } else {
            QueryBuilder zuNameBuilder = QueryBuilders.matchQuery("zuName", keyword);
            QueryBuilder buyerNameBuilder = QueryBuilders.matchQuery("buyerName", keyword);
            QueryBuilder sellerNameBuilder = QueryBuilders.matchQuery("sellerName", keyword);
            boolQueryBuilder.should(zuNameBuilder).should(buyerNameBuilder).should(sellerNameBuilder);

        }
        if (!startTime.equals("")) {
            QueryBuilder addTimeBuilder = QueryBuilders.rangeQuery("addTime").from(startTime).to(endTime);
            boolQueryBuilder.must(addTimeBuilder);
        }
        SearchResponse response = client.prepareSearch("article").setTypes("articledate")
                .setQuery(boolQueryBuilder)
                .addAggregation(AggregationBuilders.sum("price").field("price"))
                .get();
        Sum sum = response.getAggregations().get("price");
        System.out.println(sum.getValue());
    }


    /**
     * ---------------------------分页
     */


    /**
     * 使用Scroll方法分页
     */
    @Test
    public void queryPageScroll() {

        QueryBuilder qb = QueryBuilders.termQuery("id", "1");

        SearchResponse scrollResp = client.prepareSearch("article")
                .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                .setScroll(new TimeValue(60000))
                .setQuery(qb)
                .setSize(1).get();
        do {
            for (SearchHit hit : scrollResp.getHits().getHits()) {
                println(hit);
            }
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
        } while (scrollResp.getHits().getHits().length != 0);
    }


    /**
     * 分页
     *
     * @throws Exception
     */
    @Test
    public void fenye() throws Exception {

        SearchResponse response = client.prepareSearch("article")
                .setQuery(QueryBuilders.matchAllQuery())
                .setFrom(10)
                .setSize(20)
                .execute().actionGet();
        for (SearchHit searchHit : response.getHits()) {
            println(searchHit);
        }

    }


    /**
     * 高亮
     *
     * @throws Exception
     */
    @Test
    public void highlighter() throws Exception {


        QueryBuilder matchQuery = QueryBuilders.matchQuery("author", "fendo");
        HighlightBuilder hiBuilder = new HighlightBuilder();
        hiBuilder.preTags("<h2>");
        hiBuilder.postTags("</h2>");
        hiBuilder.field("author");
        // 搜索数据
        SearchResponse response = client.prepareSearch("article")
                .setQuery(matchQuery)
                .highlighter(hiBuilder)
                .execute().actionGet();
        for (SearchHit searchHit : response.getHits()) {
            println(searchHit);
        }
    }


    /**
     * ---------------------------分词器
     */

    /**
     * AnalyzeRequest 分词器
     * <a href='https://www.elastic.co/guide/cn/elasticsearch/guide/current/standard-tokenizer.html'>
     *
     * @throws Exception
     */
    @Test
    public void AnalyzeRequest() throws Exception {
        AnalyzeRequest analyzeRequest = new AnalyzeRequest();
        analyzeRequest.text("My œsophagus caused a débâcle");
        /**
         * whitespace （空白字符）分词器按空白字符 —— 空格、tabs、换行符等等进行简单拆分
         * letter 分词器 ，采用另外一种策略，按照任何非字符进行拆分
         * standard 分词器使用 Unicode 文本分割算法
         */
        analyzeRequest.addTokenFilter("standard");
        analyzeRequest.addCharFilter("asciifolding");
        ActionFuture<AnalyzeResponse> analyzeResponseActionFuture = client.admin().indices().analyze(analyzeRequest);
        List<AnalyzeResponse.AnalyzeToken> analyzeTokens = analyzeResponseActionFuture.actionGet().getTokens();
        for (AnalyzeResponse.AnalyzeToken analyzeToken : analyzeTokens) {
            System.out.println(analyzeToken.getTerm());
        }
    }

    /**
     * IK分词器
     *
     * @param args
     * @throws IOException
     */
    public void IKAnalyzer(String[] args) throws IOException {
        Settings settings = Settings.EMPTY;
        IKAnalyzer analyzer = new IKAnalyzer();
        String text = "中华人民共和国国歌";
        StringReader stringReader = new StringReader(text);
        TokenStream tokenStream = analyzer.tokenStream("", stringReader);
        tokenStream.reset();
        CharTermAttribute term = tokenStream.getAttribute(CharTermAttribute.class);
        while (tokenStream.incrementToken()) {
            System.out.print(term.toString() + "—");
        }
        stringReader.close();
        tokenStream.close();
    }

    /**
     * 输出结果SearchResponse
     *
     * @param response
     */
    public static void println(SearchResponse response) {
        System.err.println("*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-");
        System.err.println(
                "getFailedShards : " + response.getFailedShards() + "\n" +
                        //"getNumReducePhases : " + response.getNumReducePhases() + "\n" +
                        "getScrollId : " + response.getScrollId() + "\n" +
                        "getTookInMillis : " + response.getTookInMillis() + "\n" +
                        "getTotalShards : " + response.getTotalShards() + "\n" +
                        "getAggregations : " + response.getAggregations() + "\n" +
                        "getProfileResults : " + response.getProfileResults() + "\n" +
                        "getShardFailures : " + response.getShardFailures() + "\n" +
                        "getSuggest : " + response.getSuggest() + "\n" +
                        "getTook : " + response.getTook() + "\n" +
                        "isTerminatedEarly : " + response.isTerminatedEarly() + "\n" +
                        "isTimedOut : " + response.isTimedOut() + "\n" +
                        "remoteAddress : " + response.remoteAddress() + "\n" +
                        "status : " + response.status() + "\n" +
                        "getHits : " + response.getHits()
        );
    }

    /**
     * 输出结果SearchResponse
     */
    public static void println(SearchHit searchHit) {
        System.err.println("*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-");
        System.err.println(
                "docId : " +
                        // searchHit.docId() + "\n" +
                        "getId : " + searchHit.getId() + "\n" +
                        "getIndex : " + searchHit.getIndex() + "\n" +
                        "getScore : " + searchHit.getScore() + "\n" +
                        "getSourceAsString : " + searchHit.getSourceAsString() + "\n" +
                        "getType : " + searchHit.getType() + "\n" +
                        "getVersion : " + searchHit.getVersion() + "\n" +
                        // "fieldsOrNull : " + searchHit.fieldsOrNull() + "\n" +
                        "getExplanation : " + searchHit.getExplanation() + "\n" +
                        "getFields : " + searchHit.getFields() + "\n" +
                        "highlightFields : " + searchHit.highlightFields() + "\n" +
                        "hasSource : " + searchHit.hasSource()
        );
    }

}
