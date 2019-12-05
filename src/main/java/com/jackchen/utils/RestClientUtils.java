package com.jackchen.utils;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.SearchTemplateRequest;
import org.elasticsearch.script.mustache.SearchTemplateResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * restClient高级客户端，推荐使用
 */
public class RestClientUtils {

    private RestHighLevelClient client = null;

    public RestClientUtils() {
        if (client == null){
            synchronized (RestHighLevelClient.class){
                if (client == null){
                    client = getClient();
                }
            }
        }
    }

    private RestHighLevelClient getClient(){
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
//                        new HttpHost("172.24.112.17", 9200, "http"),
                        new HttpHost("127.0.0.1", 9200, "http")));

        return client;
    }

    public void closeClient(){
        try {
            if (client != null){
                client.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /*------------------------------------------------ document Api start --------------------------------------------*/

    /**
     * 增，改数据
     * @param indexName index名字
     * @param typeName type名字
     * @param id id
     * @param jsonStr 增加或修改的数据json字符串格式
     * @throws Exception
     */
    public void index(String indexName,String typeName,String id,String jsonStr) throws Exception{
        IndexRequest request = new IndexRequest(indexName,typeName,id);

        request.source(jsonStr, XContentType.JSON);
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);//同步

        String index = indexResponse.getIndex();
        String type = indexResponse.getType();
        String ID = indexResponse.getId();
        long version = indexResponse.getVersion();
        if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
            System.out.println("index: " + index);
            System.out.println("type: " + type);
            System.out.println("id: " + ID);
            System.out.println("version: " + version);
            System.out.println("status: " + "CREATED");
        } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            System.out.println("index: " + index);
            System.out.println("type: " + type);
            System.out.println("id: " + ID);
            System.out.println("version: " + version);
            System.out.println("status: " + "UPDATED");
        }
        ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
        if (shardInfo.getFailed() > 0) {
            for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                String reason = failure.reason();
            }
        }
    }

    /**
     * 根据 id 获取数据
     * @throws Exception
     */
    public void get(String indexName,String typeName,String id) throws Exception{
        GetRequest request = new GetRequest(indexName, typeName, id);

        //可以自定义要查询的具体key
        /*request.fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE);
        String[] includes = new String[]{"name", "price"};
        String[] excludes = Strings.EMPTY_ARRAY;
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
        request.fetchSourceContext(fetchSourceContext);*/

        //Synchronous Execution
        GetResponse getResponse = client.get(request, RequestOptions.DEFAULT);

        //Get Response
        String index = getResponse.getIndex();
        String type = getResponse.getType();
        String ID = getResponse.getId();
        if (getResponse.isExists()) {
            long version = getResponse.getVersion();
            String sourceAsString = getResponse.getSourceAsString();
            System.out.println("index: " + index);
            System.out.println("type: " + type);
            System.out.println("id: " + ID);
            System.out.println("version: " + version);
            System.out.println(sourceAsString);
        } else {
            System.out.println("没有查询到结果");
        }
    }

    /**
     * 存在
     * @throws Exception
     */
    public boolean exists(String indexName,String typeName,String id) throws Exception{
        GetRequest getRequest = new GetRequest(indexName, typeName, id);
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");

        //Synchronous Execution
        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
        return exists;
    }

    public void delete(String indexName,String typeName,String id) throws Exception{
        DeleteRequest request = new DeleteRequest(indexName, typeName, id);

        //Synchronous Execution
        DeleteResponse deleteResponse = client.delete(request, RequestOptions.DEFAULT);

        // document was not found
        if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
            System.out.println("要删除的数据不存在");
        }else {
            //Delete Response
            String index = deleteResponse.getIndex();
            String type = deleteResponse.getType();
            String ID = deleteResponse.getId();
            long version = deleteResponse.getVersion();
            System.out.println("index: " + index);
            System.out.println("type: " + type);
            System.out.println("id: " + ID);
            System.out.println("version: " + version);
            System.out.println("status: " + "DELETE");
            ReplicationResponse.ShardInfo shardInfo = deleteResponse.getShardInfo();
            if (shardInfo.getFailed() > 0) {
                for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                    String reason = failure.reason();
                }
            }
        }
    }

    public void update(String indexName, String typeName, String id, String jsonStr) throws Exception {
        UpdateRequest request = new UpdateRequest(indexName, typeName, id);

        request.doc(jsonStr, XContentType.JSON);
        try{
            //Synchronous Execution
            UpdateResponse updateResponse = client.update(request, RequestOptions.DEFAULT);

            //update Response
            String index = updateResponse.getIndex();
            String type = updateResponse.getType();
            String ID = updateResponse.getId();
            long version = updateResponse.getVersion();
            if (updateResponse.getResult() == DocWriteResponse.Result.CREATED) {
                System.out.println("index: " + index);
                System.out.println("type: " + type);
                System.out.println("id: " + ID);
                System.out.println("version: " + version);
                System.out.println("status: " + "CREATED");
            } else if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                System.out.println("index: " + index);
                System.out.println("type: " + type);
                System.out.println("id: " + ID);
                System.out.println("version: " + version);
                System.out.println("status: " + "UPDATED");
            } else if (updateResponse.getResult() == DocWriteResponse.Result.DELETED) {
                System.out.println("index: " + index);
                System.out.println("type: " + type);
                System.out.println("id: " + ID);
                System.out.println("version: " + version);
                System.out.println("status: " + "DELETED");
            } else if (updateResponse.getResult() == DocWriteResponse.Result.NOOP) {
                System.out.println("index: " + index);
                System.out.println("type: " + type);
                System.out.println("id: " + ID);
                System.out.println("version: " + version);
                System.out.println("status: " + "NOOP");
            }
        }catch (ElasticsearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                System.out.println("要修改的内容不存在");
            }
        }
    }

    /**
     * 根据id批量获取数据
     * @throws Exception
     */
    public void multiGet(String indexName,String typeName,String ...ids) throws Exception{
        MultiGetRequest request = new MultiGetRequest();

        for(String str:ids){
            request.add(new MultiGetRequest.Item(indexName, typeName, str));
        }

        //Synchronous Execution
        MultiGetResponse responses = client.mget(request, RequestOptions.DEFAULT);

        //Multi Get Response
        MultiGetItemResponse[] Items = responses.getResponses();
        for(MultiGetItemResponse Item:Items){
            GetResponse response = Item.getResponse();
            String index = response.getIndex();
            String type = response.getType();
            String id = response.getId();
            if (response.isExists()) {
                long version = response.getVersion();
                String sourceAsString = response.getSourceAsString();
                System.out.println("index: " + index);
                System.out.println("type: " + type);
                System.out.println("id: " + id);
                System.out.println("version: " + version);
                System.out.println(sourceAsString+"\n");
            } else {
                System.out.println("不存在");
            }
        }
    }

    /**
     * 单次请求批量处理数据,可以同时操作增删改
     * 注意：bulk方法操作增删改时只能用json格式，其他类似map方式会报错
     * @throws Exception
     */
    public void bulk() throws Exception{
        BulkRequest request = new BulkRequest();
        String jsonString="{\n" +
                "  \"name\":\"dior chengyi\",\n" +
                "  \"desc\":\"shishang gaodang\",\n" +
                "  \"price\":7000,\n" +
                "  \"producer\":\"dior producer\",\n" +
                "  \"tags\":[\"shishang\",\"shechi\"]\n" +
                "}";
        request.add(new IndexRequest("lyh_index", "user", "1")
                .source(jsonString,XContentType.JSON));
        String updateJson="{\n" +
                "  \"other\":\"test1\"\n" +
                "}";
        request.add(new UpdateRequest("lyh_index", "user", "2")
                .doc(updateJson,XContentType.JSON));
        request.add(new DeleteRequest("lyh_index", "user", "3"));

        BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
        for (BulkItemResponse bulkItemResponse : bulkResponse) {
            DocWriteResponse itemResponse = bulkItemResponse.getResponse();

            if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.INDEX
                    || bulkItemResponse.getOpType() == DocWriteRequest.OpType.CREATE) {
                IndexResponse indexResponse = (IndexResponse) itemResponse;
                String index = indexResponse.getIndex();
                String type = indexResponse.getType();
                String id = indexResponse.getId();
                long version = indexResponse.getVersion();
                System.out.println("index: " + index);
                System.out.println("type: " + type);
                System.out.println("id: " + id);
                System.out.println("version: " + version);
                System.out.println("status: " + "CREATED"+"\n");
            } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.UPDATE) {
                UpdateResponse updateResponse = (UpdateResponse) itemResponse;
                String index = updateResponse.getIndex();
                String type = updateResponse.getType();
                String id = updateResponse.getId();
                long version = updateResponse.getVersion();
                System.out.println("index: " + index);
                System.out.println("type: " + type);
                System.out.println("id: " + id);
                System.out.println("version: " + version);
                System.out.println("status: " + "UPDATE"+"\n");
            } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.DELETE) {
                DeleteResponse deleteResponse = (DeleteResponse) itemResponse;
                String index = deleteResponse.getIndex();
                String type = deleteResponse.getType();
                String id = deleteResponse.getId();
                long version = deleteResponse.getVersion();
                System.out.println("index: " + index);
                System.out.println("type: " + type);
                System.out.println("id: " + id);
                System.out.println("version: " + version);
                System.out.println("status: " + "DELETE"+"\n");
            }
        }
    }

    /*------------------------------------------------ document Api end ----------------------------------------------*/
    /*------------------------------------------------ search Api 多条件查询 start ----------------------------------------------*/
    /**
     * 查询模板
     * @throws Exception
     */
    public void searchTemplate(String indexName, String JsonStr, Map<String, Object> scriptParams) throws Exception{
        //Inline Templates
        SearchTemplateRequest request = new SearchTemplateRequest();
        request.setRequest(new SearchRequest(indexName));
        request.setScriptType(ScriptType.INLINE);
        request.setScript(JsonStr);

        request.setScriptParams(scriptParams);

        //Synchronous Execution
        SearchTemplateResponse response = client.searchTemplate(request, RequestOptions.DEFAULT);

        //SearchTemplate Response
        SearchResponse searchResponse = response.getResponse();
        //Retrieving SearchHits 获取结果数据
        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        float maxScore = hits.getMaxScore();
        System.out.println("totalHits: " + totalHits);
        System.out.println("maxScore: " + maxScore);
        System.out.println("------------------------------------------");
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            // do something with the SearchHit
            String index = hit.getIndex();
            String type = hit.getType();
            String id = hit.getId();
            float score = hit.getScore();

            String sourceAsString = hit.getSourceAsString();
            System.out.println("index: " + index);
            System.out.println("type: " + type);
            System.out.println("id: " + id);
            System.out.println("score: " + score);
            System.out.println(sourceAsString);
            System.out.println("------------------------------------------");
        }
        //得到aggregations下内容
        Aggregations aggregations = searchResponse.getAggregations();
        if(aggregations!=null){
            Map<String, Aggregation> aggregationMap = aggregations.getAsMap();
            Terms companyAggregation = (Terms) aggregationMap.get("group_by_tags");
            List<? extends Terms.Bucket> buckets = companyAggregation.getBuckets();
            for(Terms.Bucket bk:buckets){
                Object key = bk.getKey();
                long docCount = bk.getDocCount();
                System.out.println("key: "+key.toString());
                System.out.println("doc_count: "+docCount);
            }
        }
    }
}
