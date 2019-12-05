package com.jackchen.controller;

import com.jackchen.pojo.Person;
import com.jackchen.service.PersonService;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.GeoDistanceQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.GeoDistanceSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.geo.GeoPoint;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@RestController
public class PersonController {
    @Autowired
    PersonService personService;
    @Autowired
    ElasticsearchTemplate elasticsearchTemplate;

    @GetMapping("/add")
    public Object add() {
        double lat = 39.929986;
        double lon = 116.395645;
        List<Person> personList = new ArrayList<>(50000);
        for (int i = 10000; i < 60000; i++) {
            double max = 0.0001;
            double min = 0.0001;
            Random random = new Random();
            double s = random.nextDouble() % (max - min + 1) + max;
            DecimalFormat df = new DecimalFormat("######0.000000");
            // System.out.println(s);
            String lons = df.format(s + lon);
            String lats = df.format(s + lat);
            Double dlon = Double.valueOf(lons);
            Double dlat = Double.valueOf(lats);
            Person person = new Person();
            person.setId(i);
            person.setName("名字" + i);
            person.setPhone("电话" + i);
            GeoPoint geoPoint = new GeoPoint(dlat,dlon);
//            person.setAddress(geoPoint);
//            person.setAddress(dlat + "," + dlon);
            personList.add(person);
        }
        personService.bulkIndex(personList);

//    SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(QueryBuilders.queryStringQuery("spring boot OR 书籍")).build();
//    List<Article> articles = elas、ticsearchTemplate.queryForList(se、archQuery, Article.class);
//    for (Article article : articles) {
//      System.out.println(article.toString());
//    }

        return "添加数据";
    }

    /**
     *
     geo_distance: 查找距离某个中心点距离在一定范围内的位置
     geo_bounding_box: 查找某个长方形区域内的位置
     geo_distance_range: 查找距离某个中心的距离在min和max之间的位置
     geo_polygon: 查找位于多边形内的地点。
     sort可以用来排序
     */
    @GetMapping("/query")
    @ResponseBody
    public Object query() {
        double lat = 39.929986;
        double lon = 116.395645;
        Long nowTime = System.currentTimeMillis();
        //查询某经纬度100米范围内
        GeoDistanceQueryBuilder builder = QueryBuilders.geoDistanceQuery("address").point(lat, lon)
                .distance(100, DistanceUnit.METERS);

        GeoDistanceSortBuilder sortBuilder = SortBuilders.geoDistanceSort("address",lat,lon)
                .point(lat, lon)
                .unit(DistanceUnit.METERS)
                .order(SortOrder.ASC);

        Pageable pageable = PageRequest.of(0, 50);
        NativeSearchQueryBuilder builder1 = new NativeSearchQueryBuilder().withFilter(builder).withSort(sortBuilder).withPageable(pageable);
        SearchQuery searchQuery = builder1.build();
        //queryForList默认是分页，走的是queryForPage，默认10个
        List<Person> personList = elasticsearchTemplate.queryForList(searchQuery, Person.class);
        System.out.println("耗时：" + (System.currentTimeMillis() - nowTime));
        return personList;
    }
}
