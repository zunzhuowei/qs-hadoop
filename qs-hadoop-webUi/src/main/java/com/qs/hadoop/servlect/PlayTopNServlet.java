package com.qs.hadoop.servlect;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.qs.hadoop.dao.PlayTopNDao;
import com.qs.hadoop.model.PlayTopEntity;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

/**
 * 玩法 top N servlet
 * http://127.0.0.1:8080/statTop.html
 */
public class PlayTopNServlet extends HttpServlet {

    private PlayTopNDao playTopNDao = null;

    @Override
    public void init() throws ServletException {
        playTopNDao = new PlayTopNDao();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String date = req.getParameter("date");
        System.out.println("playTopNDao = " + playTopNDao + " , " + date);
        List<PlayTopEntity> playTopNDaoList = playTopNDao.getPlayTopListByDate(Integer.parseInt(date));
        String json = JSON.toJSONString(playTopNDaoList);


        resp.setContentType("ext/html;charset=UTF-8");
        PrintWriter printWriter = resp.getWriter();
        printWriter.write(json);
        printWriter.flush();
        printWriter.close();

    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        super.doPost(req, resp);
    }

}
