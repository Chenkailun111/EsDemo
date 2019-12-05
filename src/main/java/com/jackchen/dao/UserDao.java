package com.jackchen.dao;

import com.jackchen.pojo.User;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

public interface UserDao extends ElasticsearchRepository<User,Long> {
    public Page<User> findByNameEquals(String name, Pageable pageable);
}
