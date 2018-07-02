package com.qs.game.entity;

import lombok.Data;
import lombok.experimental.Accessors;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;

/**
 * Created by zun.wei on 2018/7/2 11:15.
 * Description:
 */
@Document(indexName = "product", type = "book")
@Data
@Accessors(chain = true)//链式的操作方式
public class Book {

    @Id
    String id;
    String name;
    String message;
    String type;

}
