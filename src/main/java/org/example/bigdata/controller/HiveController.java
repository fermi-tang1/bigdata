package org.example.bigdata.controller;

import org.apache.commons.lang.StringUtils;
import org.example.bigdata.model.VO.AvgRateByAgeVO;
import org.example.bigdata.model.VO.MovieVO;
import org.example.bigdata.model.VO.PopularMovieVO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Fermi.Tang
 * @Date: Created in 14:19,2022/8/1
 */

@RestController
@RequestMapping(value = "/hive")
public class HiveController {

    @Autowired
    DataSource dataSource;
    @GetMapping("/{movieId}")
    public List<AvgRateByAgeVO> getAvgRateByMovieIdGroupByAge(@PathVariable("movieId") Integer movieId) throws SQLException {
        List<AvgRateByAgeVO> avgRateByAgeVOList = new ArrayList<>();
        Connection conn = dataSource.getConnection();
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("select u.age age, avg(r.rate) rate  from t_rating r \n" +
            "left join t_user u on u.user_id = r.user_id\n" +
            "where r.movie_id = " + movieId + "\n" +
            "group by u.age");
        while (rs.next()) {
            avgRateByAgeVOList.add(AvgRateByAgeVO.builder().age(rs.getString("age")).rate(rs.getString("rate")).build());
        }
        stat.close();
        conn.close();
        return avgRateByAgeVOList;
    }

    @GetMapping("/total")
    public List<PopularMovieVO> getPopularMovies() throws SQLException {
        List<PopularMovieVO> list = new ArrayList<>();
        String sql = "select sub.movie_name,sub.avgRate, sub.counts from\n" +
            "(select m.movie_name as movie_name , avg(r.rate) as avgRate, count(*) as counts from t_rating r \n" +
            "left join t_movie m on m.movie_id = r.movie_id\n" +
            "left join t_user u on u.user_id = r.user_id\n" +
            "where u.sex = 'M'\n" +
            "group by m.movie_name ) as sub \n" +
            "where sub.counts > 50\n" +
            "order by sub.avgRate desc limit 10";
        Connection conn = dataSource.getConnection();
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery(sql);
        while (rs.next()) {
            list.add(PopularMovieVO.builder().movieName(rs.getString("sub.movie_name")).avgRate(rs.getString("sub.avgRate")).total(rs.getString("sub.counts")).build());
        }
        stat.close();
        conn.close();
        return list;
    }

    @GetMapping("/top")
    public List<MovieVO> getMovies() throws SQLException {
        List<MovieVO> list = new ArrayList<>();
        String sql = "select r.user_id, count(*) from t_rating r\n" +
            "left join t_user u on u.user_id = r.user_id\n" +
            "where u.sex = 'F'\n" +
            "group by r.user_id\n" +
            "order by count(*) desc limit 1";
        Connection conn = dataSource.getConnection();
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery(sql);
        int userId = 0;
        while (rs.next()) {
            userId = rs.getInt("r.user_id");
        }
        String sql1 = "select r.movie_id from t_rating r \n" +
            "where r.user_id = " + userId + "\n" +
            "order by r.rate desc limit 10";
        List<Integer> movieIds = new ArrayList<>();
        ResultSet rs1 = stat.executeQuery(sql1);
        while (rs1.next()) {
            movieIds.add(rs1.getInt("r.movie_id"));
        }
        String sql2 = "select m.movie_name, avg(r.rate) as avg from t_rating r \n" +
            "left join t_movie m on m.movie_id = r.movie_id\n" +
            "where r.movie_id in ( " + StringUtils.join(movieIds,",") + ")\n" +
            "group by m.movie_name";
        ResultSet rs2 = stat.executeQuery(sql2);
        while (rs2.next()) {
            list.add(MovieVO.builder().movieName(rs2.getString("m.movie_name")).avgRate(rs2.getString("avg")).build());
        }
        stat.close();
        conn.close();
        return list;
    }
}
