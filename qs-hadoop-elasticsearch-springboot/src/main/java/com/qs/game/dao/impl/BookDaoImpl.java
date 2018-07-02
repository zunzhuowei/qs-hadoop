package com.qs.game.dao.impl;

import com.qs.game.dao.BookDao;
import com.qs.game.entity.Book;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.data.elasticsearch.repository.support.SimpleElasticsearchRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Created by zun.wei on 2018/7/2 11:27.
 * Description:
 */
@Repository
public class BookDaoImpl extends SimpleElasticsearchRepository<Book> {



}