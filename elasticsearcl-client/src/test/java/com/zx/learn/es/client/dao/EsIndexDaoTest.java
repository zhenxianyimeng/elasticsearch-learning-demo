package com.zx.learn.es.client.dao;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.text.csv.CsvData;
import cn.hutool.core.text.csv.CsvReader;
import cn.hutool.core.text.csv.CsvRow;
import cn.hutool.core.text.csv.CsvUtil;
import com.zx.learn.es.client.model.Rating;
import org.elasticsearch.common.settings.Settings;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Description:
 *
 * @author: chixiao
 * @date: 2019-09-05
 * @time: 19:47
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class EsIndexDaoTest {

    @Autowired
    private EsIndexDao esIndexDao;

    @Test
    public void createIndex() throws IOException {
        String source = "{\n" +
                "  \"mappings\":{\n" +
                "\t\"ratings\": {\n" +
                "\t\t\"properties\": {\n" +
                "\t\t\t\"id\": {\n" +
                "\t\t\t\t\"type\": \"integer\"\n" +
                "\t\t\t},\n" +
                "\t\t\t\"title\": {\n" +
                "\t\t\t\t\"analyzer\": \"ik_max_word\",\n" +
                "\t\t\t\t\"type\": \"text\"\n" +
                "\t\t\t},\n" +
                "\t\t\t\"userId\": {\n" +
                "\t\t\t\t\"type\": \"integer\"\n" +
                "\t\t\t},\n" +
                "\t\t\t\"productId\": {\n" +
                "\t\t\t\t\"type\": \"integer\"\n" +
                "\t\t\t},\n" +
                "\t\t\t\"rating\": {\n" +
                "\t\t\t\t\"type\": \"integer\"\n" +
                "\t\t\t},\n" +
                "\t\t\t\"create\": {\n" +
                "\t\t\t\t\"type\": \"date\"\n" +
                "\t\t\t},\n" +
                "\t\t\t\"comment\": {\n" +
                "\t\t\t\t\"analyzer\": \"ik_smart\",\n" +
                "\t\t\t\t\"type\": \"text\"\n" +
                "\t\t\t}\n" +
                "\t\t}\n" +
                "\t  }\n" +
                "  },\n" +
                "  \"settings\":{\n" +
                "            \"index\": {\n" +
                "                \"refresh_interval\": \"1s\",\n" +
                "                \"number_of_shards\": 3,\n" +
                "                \"max_result_window\": \"10000000\",\n" +
                "                \"number_of_replicas\": 0\n" +
                "            }\n" +
                "  }\n" +
                "}";
        boolean success = esIndexDao.createIndex("ratings", source);
        Assert.assertEquals(success, true);
    }

    @Test
    public void deleteIndex() throws IOException {
        boolean success = esIndexDao.deleteIndex("ratings");
        Assert.assertEquals(success, true);
    }

    @Test
    public void addDocument() throws IOException {
        Rating rating = new Rating(1L, "很喜欢", 15905L,452609L, 5L, new Date(1380988800L),"很好很强大,纸张超赞不是一般画册所能比拟的,图片很好,特点基本都表现了出来。物种很全");
        boolean success = esIndexDao.addDocument("ratings", "1", rating);
        Assert.assertEquals(success, success);
    }

    @Test
    public void addDocuments() throws IOException {
        CsvReader reader = CsvUtil.getReader();
        CsvData data = reader.read(FileUtil.file("/Users/zjb/tmp/data/yf_amazon/ratings.csv"));
        List<CsvRow> rowList = data.getRows();
        for (CsvRow csvRow : rowList){
            List<String> list = csvRow.getRawList();
            String userId = list.get(0).replaceAll(" ","");
            System.out.println(userId);
        }
    }
}