package com.zx.learn.es.client.dao;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.text.csv.CsvData;
import cn.hutool.core.text.csv.CsvReader;
import cn.hutool.core.text.csv.CsvRow;
import cn.hutool.core.text.csv.CsvUtil;
import com.alibaba.fastjson.JSON;
import com.zx.learn.es.client.model.Rating;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.SearchTemplateRequest;
import org.elasticsearch.script.mustache.SearchTemplateResponse;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.PostConstruct;
import java.io.*;
import java.util.ArrayList;
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

    private RestHighLevelClient client;



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
        Rating rating = new Rating(1L, "很喜欢", 15905L, 452609L, 5L, new Date(1380988800L), null);
        boolean success = esIndexDao.addDocument("my_ratings", "1", rating);
        Assert.assertEquals(success, success);
    }

    @Test
    public void addDocuments() throws IOException {
        CsvReader reader = CsvUtil.getReader();
        CsvData data = reader.read(FileUtil.file("/Users/zjb/tmp/data/yf_amazon/ratings.csv"));
        List<CsvRow> rowList = data.getRows();
        for (CsvRow csvRow : rowList) {
            List<String> list = csvRow.getRawList();
            String userId = list.get(0).replaceAll(" ", "");
            System.out.println(userId);
        }
    }

    @Test
    public void addDocumentsByLines() {
        File csv = new File("/Users/zjb/tmp/data/yf_amazon/ratings.csv");  // CSV文件路径
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(csv));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        String line = "";
        String everyLine = "";
        try {
            List<String> allString = new ArrayList<>();
            int i=0;
            while ((line = br.readLine()) != null && i<=10000)  //读取到的内容给line变量
            {
                i++;
                if(i==1)
                    continue;
                String[] arrs = line.split(",");
                System.out.println(line);
                try {
                    Rating rating = new Rating(Long.valueOf(i), arrs[4], Long.valueOf(arrs[0]),Long.valueOf(arrs[1]),Long.valueOf(arrs[2]),
                            new Date(Long.valueOf(arrs[3])), arrs[5].replace("\"",""));
                    esIndexDao.addDocument("ratings", rating.getId()+"", rating);
                    //add es
                }catch (Exception e1){
                    e1.printStackTrace();
                }

            }
            System.out.println("csv表格中所有行数：" + allString.size());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void searchDocuments() throws IOException {
        String script = "{\n" +
                "  \"query\": {\"match\": {\n" +
                "    \"comment\": \"强大\"\n" +
                "  }}\n" +
                "}";

        SearchRequest searchRequest = new SearchRequest("my_ratings");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());


        searchRequest.source(searchSourceBuilder);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

        System.out.println(response);
    }
}