package com.qs.game.controller;

import com.qs.game.dao.BookDao;
import com.qs.game.entity.Book;
import io.swagger.annotations.*;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.web.bind.annotation.*;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Created by zun.wei on 2018/7/2 11:21.
 * Description:
 */
@RestController
@RequestMapping("/book/")
//@EnableSwagger2
@Api(tags = {"this is book controller"})//这里应该用英文，中文会在文档中点不开
public class BookController {

    /*
    http://192.168.1.204/swagger-ui.html#/
     */

    @Autowired
    private BookDao bookDao;

    /**
     * 1、查  id
     * @param id
     * @return
     */
    @GetMapping("/get/{id}")
    public Book getBookById(@PathVariable String id) {
        //return bookDao.findOne(id);
        return bookDao.findById(id).orElse(new Book());
    }

    /**
     * 2、查  ++:全文检索（根据整个实体的所有属性，可能结果为0个）
     * @param q
     * @return
     */
    @GetMapping("/select/{q}")
    public List<Book> testSearch(@PathVariable String q) {
        QueryStringQueryBuilder builder = new QueryStringQueryBuilder(q).field("message");
        Iterable<Book> searchResult = bookDao.search(builder);
        Iterator<Book> iterator = searchResult.iterator();
        List<Book> list = new ArrayList<Book>();
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        return list;
    }

    /**
     * 3、查   +++：分页、分数、分域（结果一个也不少）
     * @param page
     * @param size
     * @param q
     * @return
     * @return
     */
    @GetMapping("/{page}/{size}/{q}")
    @ApiOperation(value="分页查找书本列表",notes="页码必须为数字，页面大小也不需要为数字，关键字随便填")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "page", value = "当前页码", required = true, paramType = "form", dataType = "int"),
            @ApiImplicitParam(name = "size", value = "页面大小", required = true, paramType = "form", dataType = "int"),
            @ApiImplicitParam(name = "q", value = "查询关键字", required = true, paramType = "form", dataType = "String")
    })
    @ApiResponses({
            @ApiResponse(code = 400, message = "请求参数没填好"),
            @ApiResponse(code = 401, message = "请求路径未授权"),
            @ApiResponse(code = 403, message = "请求路径被禁止"),
            @ApiResponse(code = 404, message = "请求路径没有或页面跳转路径不对"),
            @ApiResponse(code = 0, message = "请求成功", response = Book.class)
    })
    public List<Book> searchCity(@PathVariable Integer page, @PathVariable Integer size, @PathVariable String q) {
        // 分页参数
        Sort sort = new Sort(Sort.Direction.DESC,"id.keyword");
//        Sort sort = new Sort(Sort.Direction.DESC,"name.keyword");
        Pageable pageable = PageRequest.of(page, size, sort);

        QueryStringQueryBuilder builder = new QueryStringQueryBuilder(q);

        // 分数，并自动按分排序
        //FunctionScoreQueryBuilder functionScoreQueryBuilder = QueryBuilders.functionScoreQuery()
        //        .add(QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("name", q)),
        //                ScoreFunctionBuilders.weightFactorFunction(1000)) // 权重：name 1000分
        //        .add(QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("message", q)),
        //                ScoreFunctionBuilders.weightFactorFunction(100)); // 权重：message 100分


        // 分数、分页
        SearchQuery searchQuery = new NativeSearchQueryBuilder().withPageable(pageable)
                .withQuery(builder).build();

        Page<Book> searchPageResults = bookDao.search(searchQuery);
        return searchPageResults.getContent();

    }

    /**
     * 4、增
     * @param book
     * @return
     */
    @PostMapping("/insert")
    public Book insertBook(Book book) {
        //book = new Book().setId("1").setName("java 编程思想").setType("IT").setMessage("java is good");
        bookDao.save(book);
        return book;
    }

    /**
     * 5、删 id
     * @param id
     * @return
     */
    @DeleteMapping("/delete/{id}")
    public Book insertBook(@PathVariable String id) {
        Book book = bookDao.findById(id).orElse(new Book());
        bookDao.deleteById(id);
        return book;
    }

    /**
     * 6、改
     * @param book
     * @return
     */
    @PutMapping("/update")
    public Book updateBook(Book book) {
        bookDao.save(book);
        return book;
    }

    @GetMapping("/get/massage/{massage}")
    public String getMassage(@PathVariable String massage) {
        List<Book> book = bookDao.getFirstByMessageStartingWith(massage);
        return Objects.isNull(book) ? "is null" : book.get(0).getMessage();
    }

    @GetMapping("/get/count/{massage}")
    public Integer getCount(@PathVariable String massage) {
        Integer count = bookDao.countBooksByMessageStartingWith(massage);
        return Objects.isNull(count) ? 0 : count;
    }


}
