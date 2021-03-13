package com.angle.standalone;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class solrDemo {
    public static final String url = "http://192.168.44.214:8983/solr";
    private static HttpSolrClient client;

    static {
        client = new HttpSolrClient.Builder(url).build();
        System.out.println(client);
    }

    public static void main(String[] args) throws IOException, SolrServerException {
        addData();
    }

    private static void addOneDta() throws IOException, SolrServerException {
        ArrayList<SolrInputDocument> dcos = new ArrayList<>();
        for (int i = 6; i < 15; i++) {
            SolrInputDocument dox = new SolrInputDocument();
            dox.addField("id", i);
            dox.addField("name", "chuizi" + i);
            dox.addField("remask", "sjdajd" + i);
            dcos.add(dox);
        }
        client.add(dcos);
        client.commit();
        client.close();
        System.out.println("success!!!!");
    }

    private static void addData() throws IOException, SolrServerException {
        //一个集合一个集合的添加
        List<SolrInputDocument> persons = new ArrayList<>(5);

        SolrInputDocument lvbu = new SolrInputDocument();
        lvbu.addField("id", "1");
        lvbu.addField("name", "吕布");
        lvbu.addField("age", 33);
        lvbu.addField("sex", "男");
        lvbu.addField("salary", "8888.88");
        lvbu.addField("remark", "人中吕布,马中赤兔");

        SolrInputDocument zhaoyun = new SolrInputDocument();
        zhaoyun.addField("id", "2");
        zhaoyun.addField("name", "赵云");
        zhaoyun.addField("age", 28);
        zhaoyun.addField("sex", "男");
        zhaoyun.addField("salary", "8888.88");
        zhaoyun.addField("remark", "七进七出");

        SolrInputDocument guanyu = new SolrInputDocument();
        guanyu.addField("id", "3");
        guanyu.addField("name", "关羽");
        guanyu.addField("age", 44);
        guanyu.addField("sex", "男");
        guanyu.addField("salary", "9999.88");
        guanyu.addField("remark", "忠肝义胆");

        SolrInputDocument zhangfei = new SolrInputDocument();
        zhangfei.addField("id", "4");
        zhangfei.addField("name", "张飞");
        zhangfei.addField("age", 41);
        zhangfei.addField("sex", "男");
        zhangfei.addField("salary", "8888.88");
        zhangfei.addField("remark", "莽夫一个");


        SolrInputDocument liubei = new SolrInputDocument();
        liubei.addField("id", "5");
        liubei.addField("name", "刘备");
        liubei.addField("age", 48);
        liubei.addField("sex", "男");
        liubei.addField("salary", "99999.88");
        liubei.addField("remark", "心机婊");

        persons.add(lvbu);
        persons.add(zhaoyun);
        persons.add(guanyu);
        persons.add(zhangfei);
        persons.add(liubei);

        client.add("gettingstarted",persons);
        client.commit();
        client.close();
    }
}
