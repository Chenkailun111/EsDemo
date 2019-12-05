package com.jackchen.utils;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.missing.Missing;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * TransportClient 还是在elasticsearch7.0版本之前，现在推荐使用restClient
 */
@Component
public class AggregationUtil {

    @Autowired
//    private RestHighLevelClient client;
    private TransportClient client;
    /**
     * 分组统计
     * @param index 索引
     * @param field 文档属性
     * @return
     */
    public Terms terms(String index, String field){
        String name = "terms";
        AggregationBuilder agg= AggregationBuilders.terms(name).field(field);
        SearchResponse response=client.prepareSearch(index).addAggregation(agg).execute().actionGet();
        return response.getAggregations().get(name);
    }

    /**
     * 过滤器统计
     * @param index 索引
     * @param field 文档属性
     * @return
     */
    public Filter filter(String index, String field, String key){
        QueryBuilder query= QueryBuilders.termQuery(field,key);
        String name ="filter";
        AggregationBuilder agg=AggregationBuilders.filter(name,query);
        SearchResponse response=client.prepareSearch(index).addAggregation(agg).execute().actionGet();
        return response.getAggregations().get(name);
    }

    /**
     * 多过滤器统计
     * @param index 索引
     * @param list  过滤条件队列
     * @return
     */
    public Filters filters(String index, List<TermQueryBuilder> list){
        String name = "filters";

        FiltersAggregator.KeyedFilter[] filters = new FiltersAggregator.KeyedFilter[list.size()];
        for(int i=0; i <list.size(); i++){
            TermQueryBuilder obj = list.get(i);
            FiltersAggregator.KeyedFilter item = new FiltersAggregator.KeyedFilter(obj.fieldName(),obj);
            filters[i] = item;
        }

        AggregationBuilder agg=AggregationBuilders.filters(name, filters);
        SearchResponse response=client.prepareSearch(index).addAggregation(agg).execute().actionGet();
        return response.getAggregations().get(name);
    }

    /**
     * 区间统计
     * @param index 索引
     * @param field 文档属性
     * @return
     */
    public Range range(String index, String field, double to, double from){
        String name ="range";
        AggregationBuilder agg=AggregationBuilders
                .range(name)
                .field(field)
                .addUnboundedTo(to)//第1个范围 ( ,to)
                .addRange(to,from)//第2个范围[to,from)
                .addUnboundedFrom(from);//第3个范围[from,)
        SearchResponse response=client.prepareSearch(index).addAggregation(agg).execute().actionGet();
        return response.getAggregations().get(name);
    }



    /**
     * 日期区间统计
     * @param index 索引
     * @param field 文档属性
     * @return
     */
    public Range dateRange(String index, String field,String to,String from, String formated){
        String name ="dateRange";
        AggregationBuilder agg=AggregationBuilders
                .dateRange(name)
                .field(field)
                .format(formated)
                .addUnboundedTo(to)
                .addUnboundedFrom(from);
        SearchResponse response=client.prepareSearch(index).addAggregation(agg).execute().actionGet();
        return response.getAggregations().get(name);
    }

    /**
     * Missing统计
     * @param field
     * @return
     */
    public Missing missing(String index, String field){
        String name ="missing";
        AggregationBuilder agg=AggregationBuilders.missing(name).field(field);
        SearchResponse response=client.prepareSearch(index).addAggregation(agg).execute().actionGet();
        return response.getAggregations().get(name);
    }

}
