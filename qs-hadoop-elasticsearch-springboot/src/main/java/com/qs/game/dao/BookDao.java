package com.qs.game.dao;

import com.qs.game.entity.Book;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

/**
 * Created by zun.wei on 2018/7/2 11:19.
 * Description:
 */
public interface BookDao extends ElasticsearchRepository<Book, String> {

    List<Book> getFirstByMessageStartingWith(String message);

    Integer countBooksByMessageStartingWith(String message);
}
