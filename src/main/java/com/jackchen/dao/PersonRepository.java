package com.jackchen.dao;

import com.jackchen.pojo.Person;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

public interface PersonRepository extends ElasticsearchRepository<Person,Integer> {
}
