import com.jackchen.EsApplication;
import com.jackchen.dao.UserDao;
import com.jackchen.pojo.Person;
import com.jackchen.pojo.User;
import com.jackchen.service.PersonService;
import com.jackchen.utils.JacksonUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoShapeType;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.range.GeoDistanceAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.GeoDistanceSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cglib.core.ReflectUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.SearchResultMapper;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.aggregation.impl.AggregatedPageImpl;
import org.springframework.data.elasticsearch.core.query.*;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.text.DecimalFormat;
import java.util.*;


@RunWith(SpringRunner.class)
@SpringBootTest(classes = {EsApplication.class})
public class TestSpringBootES {

    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    @Test
    public void testSave() {
        User user = new User();
        user.setId(1001L);
        user.setAge(20);
        user.setName("张三");
        user.setHobby("足球、篮球、听音乐");
        IndexQuery indexQuery = new IndexQueryBuilder().withObject(user).build();
        //建立索引
        String index = this.elasticsearchTemplate.index(indexQuery);
        System.out.println(index);
    }

    @Test
    public void testBulk() {
        List list = new ArrayList();
        for (int i = 0; i < 5000; i++) {
            User user = new User();
            user.setId(1001L + i);
            user.setAge(i % 50 + 10);
            user.setName("张三" + i);
            user.setHobby("足球、篮球、听音乐");

            IndexQuery indexQuery = new IndexQueryBuilder().withObject(user).build();
            list.add(indexQuery);
        }
        Long start = System.currentTimeMillis();
        //批量插入索引数据
        this.elasticsearchTemplate.bulkIndex(list);
        System.out.println("用时：" + (System.currentTimeMillis() - start)); //用时
    }


    /**
     * 局部更新，全部更新使用index覆盖即可
     */
    @Test
    public void testUpdate() {
        IndexRequest indexRequest = new IndexRequest();
        indexRequest.source("age", "30");
        UpdateQuery updateQuery = new UpdateQueryBuilder()
                .withId("1001")
                .withClass(User.class)
                .withIndexRequest(indexRequest).build();
        this.elasticsearchTemplate.update(updateQuery);
    }

    //删除
    @Test
    public void testDelete() {
        elasticsearchTemplate.delete(User.class, "1001");
    }

    //搜索
    @Test
    public void testSearch() {
        PageRequest pageRequest = PageRequest.of(2, 10); //设置分页参数
        //分页查询构建
        SearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withQuery(QueryBuilders.matchQuery("name", "张三")) // match查询
                .withPageable(pageRequest)
                .build();
        AggregatedPage<User> users =
                this.elasticsearchTemplate.queryForPage(searchQuery, User.class);
        System.out.println("总页数：" + users.getTotalPages()); //获取总页数
        for (User user : users.getContent()) { // 获取搜索到的数据
            System.out.println(user);
        }
    }

    @Autowired
    private UserDao userDao;

    @Test
    public void testRepositroySearch() {
        PageRequest pageRequest = PageRequest.of(1, 10);
        Page<User> pageList = userDao.findByNameEquals("张三", pageRequest);
        System.out.println("总记录数" + pageList.getTotalElements());
        System.out.println("内容" + pageList.getContent());
        System.out.println("总页数" + pageList.getTotalPages());
    }


    //geo_distance
    @Test
    public void geo_distance() {
        double lat = 39.929986;
        double lon = 116.395645;
        Long nowTime = System.currentTimeMillis();
        //查询某经纬度100米范围内
        GeoDistanceQueryBuilder builder = QueryBuilders.geoDistanceQuery("address").point(lat, lon)
                .distance(100, DistanceUnit.METERS);

        GeoDistanceSortBuilder sortBuilder = SortBuilders.geoDistanceSort("address", lat, lon)
                .point(lat, lon)
                .unit(DistanceUnit.METERS)
                .order(SortOrder.ASC);

        Pageable pageable = PageRequest.of(1, 10);
        NativeSearchQueryBuilder builder1 = new NativeSearchQueryBuilder().withFilter(builder).withSort(sortBuilder).withPageable(pageable);
        SearchQuery searchQuery = builder1.build();
        //queryForList默认是分页，走的是queryForPage，默认10个
        List<Person> personList = elasticsearchTemplate.queryForList(searchQuery, Person.class);
        for (Person person : personList) {
            System.out.println(person);
        }
        System.out.println("耗时：" + (System.currentTimeMillis() - nowTime));
    }

    // geo_bounding_box 矩形边界查询
    @Test
    public void geo_bounding_box() {
        Long nowTime = System.currentTimeMillis();
        double lat = 39.929986;
        double lon = 116.395645;
        GeoBoundingBoxQueryBuilder boundingBoxQueryBuilder =
                QueryBuilders.geoBoundingBoxQuery("address")
                        .setCorners(40.73, 116.295645, 39.01, 117.395645);

        Pageable pageable = PageRequest.of(1, 10);
        NativeSearchQueryBuilder builder = new NativeSearchQueryBuilder()
                .withFilter(boundingBoxQueryBuilder)
                .withPageable(pageable);
        //返回搜索列表
        SearchQuery searchQuery = builder.build();
        List<Person> personList = elasticsearchTemplate.queryForList(searchQuery, Person.class);
        for (Person person : personList) {
            System.out.println(person);
        }
        System.out.println("耗时：" + (System.currentTimeMillis() - nowTime));
    }


    //  多边形查询, 查找位于多边形内的地点
    @Test
    public void geo_polygon() {
        //1 统计耗时
        Long nowTime = System.currentTimeMillis();
        double lat = 39.929986;
        double lon = 116.395645;

        //2 构建查询
        List<GeoPoint> points = new ArrayList<GeoPoint>();
        points.add(new GeoPoint(39.929986, 100.1));
        points.add(new GeoPoint(40.73, 118.13291));
        points.add(new GeoPoint(41.63051, 121.1));

        GeoPolygonQueryBuilder polygonQueryBuilder =
                QueryBuilders.geoPolygonQuery("address", points);
        //3 构建排序
//        GeoDistanceSortBuilder sortBuilder = SortBuilders.geoDistanceSort("address",lat,lon)
//                .point(lat, lon)
//                .unit(DistanceUnit.METERS)
//                .order(SortOrder.ASC);

        System.out.println(polygonQueryBuilder.toString());
        //4 构建分页
        Pageable pageable = PageRequest.of(0, 8);
        NativeSearchQueryBuilder builder =
                new NativeSearchQueryBuilder()
                        .withFilter(polygonQueryBuilder)
                        .withPageable(pageable);
        NativeSearchQuery searchQuery = builder.build();
        List<Person> personList = elasticsearchTemplate.queryForList(searchQuery, Person.class);
        System.out.println("查询出来的总长度：" + personList.size()); //结果表明是分页的当前页的长度8
        for (Person person : personList) {
            System.out.println(person);
        }
        System.out.println("耗时：" + (System.currentTimeMillis() - nowTime));
    }

    //范围查询，比如id 60000 -》70000
    @Test
    public void geo_range() {
        Long nowTime = System.currentTimeMillis();
        double lat = 39.929986;
        double lon = 116.395645;

        RangeQueryBuilder queryBuilder = QueryBuilders.rangeQuery("id")
                .from(60000)
                .to(70000);
        NativeSearchQueryBuilder builder =
                new NativeSearchQueryBuilder()
                        .withFilter(queryBuilder);
        NativeSearchQuery searchQuery = builder.build();

        List<Person> personList = elasticsearchTemplate.queryForList(searchQuery, Person.class);
        System.out.println("查询出来的总长度：" + personList.size()); //即使不自己分页，默认有分页大小10
        for (Person person : personList) {
            System.out.println(person);
        }
        System.out.println("耗时：" + (System.currentTimeMillis() - nowTime));
    }

    //id聚合查询
    @Test
    public void agg_id() {
        RangeAggregationBuilder aggregationBuilder = AggregationBuilders
                .range("aag_id")
                .field("id")
                .addRange(60000, 62000)
                .addRange(62000, 65000);
        System.out.println(aggregationBuilder.toString());

        NativeSearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withPageable(PageRequest.of(0, 15))
                .addAggregation(aggregationBuilder).build();
        AggregatedPage<Person> personList = elasticsearchTemplate.queryForPage(searchQuery, Person.class);
        Range range = (Range) personList.getAggregation("aag_id");
        for (Range.Bucket entry : range.getBuckets()) {
            String key = entry.getKeyAsString();    // key as String
            Number from = (Number) entry.getFrom(); // bucket from value
            Number to = (Number) entry.getTo();     // bucket to value
            long docCount = entry.getDocCount();    // Doc count

//        logger.info("key [{}], from [{}], to [{}], doc_count [{}]", key, from, to, docCount);
            System.out.println("key:" + key + " from:" + from + " to:" + to + " docCount" + docCount);
        }
        System.out.println(personList.getSize() + ":::::::" + personList.getTotalPages());
    }

    //进行条件聚合范围查询
    @Test
    public void aggregation() {
        Long nowTime = System.currentTimeMillis();
        double lat = 39.929986;
        double lon = 116.395645;

        GeoDistanceAggregationBuilder aggregationBuilder = AggregationBuilders
                .geoDistance("agg_by_distance", new GeoPoint(lat, lon))
                .field("address")
                .unit(DistanceUnit.MILES)
                .addUnboundedTo(30)
                .addRange(30, 60)
                .addRange(60, 100);

        System.out.println(aggregationBuilder.toString());

        NativeSearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withPageable(PageRequest.of(0, 15))
                .addAggregation(aggregationBuilder)
                .build();

        AggregatedPage<Person> aggregatedPage = elasticsearchTemplate.queryForPage(searchQuery, Person.class);
        Range range = (Range) aggregatedPage.getAggregation("agg_by_distance");
        for (Range.Bucket entry : range.getBuckets()) {
            String key = entry.getKeyAsString();    // key as String
            Number from = (Number) entry.getFrom(); // bucket from value
            Number to = (Number) entry.getTo();     // bucket to value
            long docCount = entry.getDocCount();    // Doc count

//        logger.info("key [{}], from [{}], to [{}], doc_count [{}]", key, from, to, docCount);
            System.out.println("key:" + key + " from:" + from + " to:" + to + " docCount" + docCount);
        }
        System.out.println(aggregatedPage.getSize() + ":::::::" + aggregatedPage.getTotalPages());

        System.out.println("内容：" + aggregatedPage.getContent());

        System.out.println("耗时：" + (System.currentTimeMillis() - nowTime));
    }

    //高亮处理
    @Test
    public void highlight() {
        //查询语句
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery("60000", "name", "phone");

        //构建器
        NativeSearchQueryBuilder builder = new NativeSearchQueryBuilder()
                .withQuery(multiMatchQueryBuilder)
                .withHighlightFields(new HighlightBuilder.Field("name"), new HighlightBuilder.Field("phone"))
                .withPageable(PageRequest.of(0, 5));

        System.out.println(builder.toString());
        NativeSearchQuery searchQuery = builder.build();

        AggregatedPage<Person> aggregatedPage = elasticsearchTemplate.queryForPage(searchQuery, Person.class, new SearchResultMapper() {
            @Override
            public <T> AggregatedPage<T> mapResults(SearchResponse response, Class<T> clazz, Pageable pageable) {
                if (response.getHits().totalHits == 0) {
                    return new AggregatedPageImpl<>(Collections.emptyList(), pageable, 0L);
                }

                List<T> list = new ArrayList<>();
                for (SearchHit searchHit : response.getHits().getHits()) {
                    T obj = (T) ReflectUtils.newInstance(clazz);

                    //id字段写入，这个内部方法只能手动写入高亮和非高亮信息
                    try {
                        FieldUtils.writeField(obj, "id", Integer.parseInt(searchHit.getId()), true);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    }

                    // 非高亮字段的数据写入
                    for (Map.Entry<String, Object> entry : searchHit.getSourceAsMap().entrySet()) {

                        Field field = FieldUtils.getField(clazz, entry.getKey(), true);
                        if (null == field) {
                            continue; //此处不做操作，在对空字段处理的时候会报错
                        }

                        try {
                            FieldUtils.writeField(obj, entry.getKey(), entry.getValue(), true);
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }
                    }
                    //高亮字段进行处理
                    for (Map.Entry<String, HighlightField> entry : searchHit.getHighlightFields().entrySet()) {
                        StringBuilder sb = new StringBuilder();
                        Text[] fragments = entry.getValue().getFragments();
                        for (Text fragment : fragments) {
                            sb.append(fragment.toString());
                        }

                        // 写入高亮的内容
                        try {
                            FieldUtils.writeField(obj, entry.getKey(), sb.toString(), true);
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }
                    }

                    list.add(obj);
                }
                return new AggregatedPageImpl<>(list, pageable, response.getHits().totalHits);
            }
        });

        System.out.println(aggregatedPage.getSize() + ":::::::" + aggregatedPage.getTotalPages());
        System.out.println("内容：" + aggregatedPage.getContent());

    }


    //实体类转换成为map
    public  Map<String, Object> objectToMap(Object obj) {
        Map<String, Object> map = new HashMap<>();
        if (obj == null) {
            return map;
        }
        Class clazz = obj.getClass();
        Field[] fields = clazz.getDeclaredFields();
        try {
            for (Field field : fields) {
                field.setAccessible(true);
                map.put(field.getName(), field.get(obj));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return map;
    }



    //高亮处理
    @Test
    public void highlight2() {
        //查询语句
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery("60000", "name", "phone");

        //构建器
        NativeSearchQueryBuilder builder = new NativeSearchQueryBuilder()
                .withQuery(multiMatchQueryBuilder)
                .withHighlightFields(new HighlightBuilder.Field("phone"))
                .withPageable(PageRequest.of(0, 5));

        System.out.println(builder.toString());
        NativeSearchQuery searchQuery = builder.build();

        AggregatedPage<Person> aggregatedPage = elasticsearchTemplate.queryForPage(searchQuery, Person.class, new SearchResultMapper() {
            @Override
            public <T> AggregatedPage<T> mapResults(SearchResponse response, Class<T> clazz, Pageable pageable) {
                if (response.getHits().totalHits == 0) {
                    return new AggregatedPageImpl<>(Collections.emptyList(), pageable, 0L);
                }

                List<T> list = new ArrayList<>();
                for (SearchHit searchHit : response.getHits().getHits()) {
                    Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
                    Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
                    Person person = createEsDoc(sourceAsMap, highlightFields);
                    list.add((T)person);
                }
                return new AggregatedPageImpl<>(list, pageable, response.getHits().totalHits);
            }
        });

        System.out.println(aggregatedPage.getSize() + ":::::::" + aggregatedPage.getTotalPages());
        System.out.println("内容：" + aggregatedPage.getContent());

    }
    /**
     * 添加高亮条件
     * @param searchQuery
     */
    private void withHighlight(NativeSearchQueryBuilder searchQuery){
        HighlightBuilder.Field hfield= new HighlightBuilder.Field("name")
                .preTags("<em style='color:red'>")
                .postTags("</em>")
                .fragmentSize(100);

        HighlightBuilder.Field hfield1= new HighlightBuilder.Field("phone")
                .preTags("<em style='color:yellow'>")
                .postTags("</em>")
                .fragmentSize(100);
        searchQuery.withHighlightFields(hfield,hfield1);
    }

    /**
     * 根据搜索结果创建esdoc对象
     * @param smap
     * @param hmap
     * @return
     */
    private Person createEsDoc(Map<String, Object> smap, Map<String, HighlightField> hmap){
        Person ed = new Person();
        if (smap.get("name") != null)
            ed.setName(smap.get("name").toString());
        //高亮字段处理
        if (hmap.get("phone") != null)
            ed.setPhone(hmap.get("phone").fragments()[0].toString());
            else if(smap.get("phone")!=null)
             ed.setPhone(smap.get("phone").toString());
        if (smap.get("id") != null)
            ed.setId(Integer.parseInt(smap.get("id").toString()));
        if (smap.get("address") != null) {
            try {
                String address = smap.get("address").toString();
//                GeoPoint geoPoint = JacksonUtils.json2pojo(address, GeoPoint.class);
                String[] split = address.split(",");
                System.out.println("spit ：" + split);
                split[0] = split[0].substring(5);
                split[1] = split[1].substring(5,split[1].length()-1);
                System.out.println("split[0]：" + split[0] + " split[1]：" + split[1]);
                GeoPoint geoPoint = new GeoPoint(Double.parseDouble(split[1]),Double.parseDouble(split[0]));
                System.out.println("geo_point : " + geoPoint);
                ed.setAddress(geoPoint);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return ed;
    }


    @Test
    public void commonTest(){
        System.out.println("{lon=116.523042".substring(1));
    }
    @Autowired
    private PersonService personService;

    //批量添加数据
    @Test
    public void bulkIndex() {
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
           GeoPoint geoPoint = new GeoPoint(dlat, dlon);
//            person.setAddress(geoPoint);
//            person.setAddress(dlat + "," + dlon);
            personList.add(person);
        }
        personService.bulkIndex(personList);
    }


}
