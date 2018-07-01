package com.qs.game.starter;


import org.apache.logging.log4j.core.util.datetime.FastDateFormat;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;

/**
 * 创建索引api
 */
public class IndexApi {

    private static final FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        //设置集群名称
        Settings settings = Settings.builder().put("cluster.name", "my-elasticsreach-application")
                .put("client.transport.sniff",true)
                .build();// 集群名

        //创建client
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress
                        (InetAddress.getByName("192.168.1.233"), 9300));


        //createIndexMapping(client, "car_shop", "cars");//建立mapping
        //addIndexAndDocument(client, "car_shop", "cars");//添加索引并添加文档
        //upsert(client, "car_shop", "cars");//存在就更新，不存在就插入
        //mGet(client, "car_shop", "cars");//获取多个结果集
        //bulk(client, "car_shop", "cars");//批量数据处理
        scoll(client, "car_shop", "cars");//批量数据处理



        client.close();
    }


    /**
     *  滚动实现批量下载
     * @param client
     * @param index
     * @param type
     * @throws Exception
     */
    public static void scoll(TransportClient client, String index, String type) throws Exception{
        SearchResponse searchResponse = client.prepareSearch(index)
                .setTypes(type)
                .setQuery(QueryBuilders.termQuery("brand", "宝马"))
                .setSize(1)
                .setScroll(TimeValue.timeValueSeconds(10))
                .get();

        long count = 0;
        do {
            //SearchHit[] searchHits = searchResponse.getHits().getHits();
            for (SearchHit searchHit : searchResponse.getHits().getHits()) {
                System.out.println("count = " + ++count);
                System.out.println("searchHit = " + searchHit.getSourceAsString());

                //每次查询一批数据，比如1000条，然后写入指定位置
            }
            searchResponse = client.prepareSearchScroll(searchResponse.getScrollId())
                    .setScroll(TimeValue.timeValueSeconds(10))
                    .execute()
                    .actionGet();

        } while (searchResponse.getHits().getHits().length != 0);

    }

    /**
     * 批量数据处理
     * @param client
     * @param index
     * @param type
     */
    public static void bulk(TransportClient client, String index, String type) throws IOException, ExecutionException, InterruptedException {
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

        IndexRequestBuilder indexRequestBuilder = client.prepareIndex().setIndex(index).setType(type).setSource(
                XContentFactory.jsonBuilder()
                        .startObject()
                        .field("id", "3")
                        .field("name", "五菱")
                        .field("brand", "五菱宏光")
                        .field("price", "100000")
                        .field("produce_date", "2018-11-12 12:12:11")
                        .endObject());

        UpdateRequestBuilder updateRequestBuilder = client.prepareUpdate(index, type, "AWRVoPFcDpREQddx9Yix").setDoc(
                XContentFactory.jsonBuilder().startObject()
                        .field("id", "1")
                        .field("name", "五菱")
                        .field("brand", "五菱之光")
                        .field("price", "50000")
                        .field("produce_date", "2018-11-12 13:12:11")
                        .endObject());

        DeleteRequestBuilder deleteRequestBuilder = client.prepareDelete(index, type, "2");

        bulkRequestBuilder.add(indexRequestBuilder);
        bulkRequestBuilder.add(updateRequestBuilder);
        bulkRequestBuilder.add(deleteRequestBuilder);


        BulkResponse bulkItemResponses = bulkRequestBuilder.execute().get();
        Iterator<BulkItemResponse> iterable = bulkItemResponses.iterator();
        while (iterable.hasNext()) {
            BulkItemResponse bulkItemResponse = iterable.next();
            bulkItemResponses.forEach(e -> System.out.println(" e.getVersion() = " + e.getVersion()));
        }
    }

    /**
     * 获取多个结果集
     * @param client
     * @param index
     * @param type
     */
    public static void mGet(TransportClient client, String index, String type) {
        MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
                .add(new MultiGetRequest.Item(index, type, "AWRVoPFcDpREQddx9Yix"))
                .add(new MultiGetRequest.Item(index, type, "2"))
                .get(TimeValue.timeValueSeconds(30));
        for (MultiGetItemResponse multiGetItemRespons : multiGetItemResponses) {
            String result = multiGetItemRespons.getResponse().getSourceAsString();
            System.out.println("result = " + result);
        }

    }

    /**
     *  添加索引并且添加文档
     * @param client TransportClient 客户端
     * @param index 索引
     * @param type 类型
     * @throws Exception
     */
    public static void addIndexAndDocument(TransportClient client, String index, String type) throws Exception {
        Date time = new Date();
        IndexResponse response = client.prepareIndex(index, type)//prepareIndex(index, type,"1") 手动指定id
                .setSource(XContentFactory.jsonBuilder().startObject()
                        .field("id", "1")
                        .field("name", "宝马320")
                        .field("brand", "宝马")
                        .field("price", "320000")
                        .field("produce_date", fastDateFormat.format(time))
                        .endObject())
                .get();
        System.out.println("添加索引成功,版本号：" + response.getVersion());
    }



    /**
     * 如果存在就更新，不存在进就创建
     * @param client
     * @param index
     * @param type
     */
    public static void upsert(TransportClient client, String index, String type) throws IOException {
        Date time = new Date();
        UpdateRequestBuilder updateRequestBuilder = client.prepareUpdate(index,type,"2")
                .setDoc(XContentFactory.jsonBuilder().startObject()
                        .field("id", "1")
                        .field("name", "宝马740")
                        .field("price", "800000")
                        .field("produce_date", fastDateFormat.format(time))
                        .endObject());

        updateRequestBuilder.setUpsert().get();
    }



    /**
     * 建立mapping
     * @param client TransportClient 客户端
     * @param index 索引
     * @param type 类型
     */
    public static void createIndexMapping(TransportClient client, String index, String type) throws IOException {
        CreateIndexRequestBuilder cib = client.admin().indices().prepareCreate(index);
        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties") //设置之定义字段
                .startObject("brand").field("analyzer", "ik_max_word")
                .field("type", "string") //设置数据类型
                .endObject()
                .startObject("name").field("analyzer", "ik_max_word")
                .field("type", "string")
                .endObject()
                .startObject("price")
                .field("type", "string")
                .endObject()
                .startObject("produce_date")
                .field("type", "date")  //设置Date类型
                .field("format", "yyyy-MM-dd HH:mm:ss") //设置Date的格式
                .endObject()
                .endObject()
                .endObject();

        cib.addMapping(type, mapping);
        CreateIndexResponse res = cib.execute().actionGet();
        System.out.println("----------添加映射成功----------");
    }


}
