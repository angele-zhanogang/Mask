package com.angle.solrCloud;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Optional;

public class solrDemo {

    private CloudSolrClient client;
    @Before
    public void init() throws IOException {

        ArrayList<String> zkServers = new ArrayList<>();
        zkServers.add("apache1:2181");
        zkServers.add("apache2:2181");
        zkServers.add("apache3:2181");
        client = new CloudSolrClient.Builder(zkServers, Optional.of("/solr")).build();
        client.setDefaultCollection("collection2");
        client.setZkClientTimeout(5000);
        client.setZkConnectTimeout(5000);
        client.connect();

    }
    @Test
    public void addDocument() throws IOException, SolrServerException {

        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id","3");
        doc.addField("title","sad");
        client.add("collection2",doc);

        client.commit();
    }

    @Test
    public void deleteById() throws IOException, SolrServerException {
        //删除一条数据
        client.deleteById("1");
        //通过id批量删除
//        client.deleteById(new ArrayList<String>(){
//            {
//                add("1");
//                add("2");
//                add("3");
//                add("4");
//                add("5");
//            }
//        });
    }

    @Test
    public void query() throws IOException, SolrServerException {
        SolrQuery solrQuery = new SolrQuery("*:*");
        QueryResponse response = client.query(solrQuery);
        SolrDocumentList docList = response.getResults();
        for (SolrDocument doc : docList) {
            int id = Integer.parseInt(doc.get("id").toString());
            String title =  doc.get("title").toString();
            System.out.println("id"+id+"========="+"title:"+title);
        }
    }


    @After
    public void end() throws IOException {
        client.close();
    }
}
