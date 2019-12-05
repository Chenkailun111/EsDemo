package com.jackchen.service;

import com.jackchen.dao.PersonRepository;
import com.jackchen.pojo.Person;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class PersonService {
    @Autowired
    private PersonRepository personRepository;
    @Autowired
    ElasticsearchTemplate elasticsearchTemplate;

    private static final String PERSON_INDEX_NAME = "elastic_search_project";
    private static final String PERSON_INDEX_TYPE = "person";

    public Person add(Person person) {
        return personRepository.save(person);
    }

    //批量插入
    public void bulkIndex(List<Person> personList) {
        int counter = 0;
        try {
            //检查目标索引是否存在，不存在创建
            if (!elasticsearchTemplate.indexExists(PERSON_INDEX_NAME)) {
                elasticsearchTemplate.createIndex(PERSON_INDEX_NAME);
                //创建index时，显式调用一下mapping方法，才能正确的映射为geofield
                elasticsearchTemplate.putMapping(Person.class);
            }
            List<IndexQuery> queries = new ArrayList<>();
            for (Person person : personList) {
                IndexQuery indexQuery = new IndexQuery();
                indexQuery.setId(person.getId() + "");
                indexQuery.setObject(person);
                indexQuery.setIndexName(PERSON_INDEX_NAME);
                indexQuery.setType(PERSON_INDEX_TYPE);

                //上面的那几步也可以使用IndexQueryBuilder来构建
                /*IndexQuery query = new IndexQueryBuilder()
                        .withId(person.getId() + "")
                        .withObject(person)
                        .withIndexName(PERSON_INDEX_NAME)
                        .withType(PERSON_INDEX_TYPE).build();*/

                queries.add(indexQuery);
                if (counter % 500 == 0) { //每500次批量插入一次数据
                    elasticsearchTemplate.bulkIndex(queries);
                    queries.clear();
                    //看批量插入了多少次
                    System.out.println("bulkIndex counter : " + counter);
                }
                counter++;
            }
            if (queries.size() > 0) {
                elasticsearchTemplate.bulkIndex(queries);
            }
            System.out.println("bulkIndex completed.");
        } catch (Exception e) {
            System.out.println("IndexerService.bulkIndex e;" + e.getMessage());
            throw e;
        }
    }
}
