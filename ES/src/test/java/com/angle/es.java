package com.angle;

import com.alibaba.fastjson.JSON;
import com.angle.core.Person;
import com.angle.utils.EsUtils;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;
import java.util.Map;

public class es {

    private TransportClient client;

    @BeforeEach
    public void init() throws UnknownHostException {
        client = EsUtils.getClient();
    }

    @AfterEach
    public void end() {
        client.close();
    }


    /**
     * 通过id查找指定数据
     */
    @Test
    public void queryById() {
        GetResponse documentFields = client.prepareGet("indexsearch", "mysearch", "11").get();

        String index = documentFields.getIndex();
        String type = documentFields.getType();
        String id = documentFields.getId();

        System.out.println(index + "   " + type + "   " + id);

        Map<String, Object> source = documentFields.getSource();
        for (String s : source.keySet()) {
            System.out.println(source.get(s));
        }

    }

    /**
     * 查询索引库中所有数据
     */
    @Test
    public void searchAll() {
        SearchResponse searchResponse = client.prepareSearch("indexsearch")
                .setTypes("mysearch")
                .setQuery(new MatchAllQueryBuilder())
                .get();

        SearchHits hits = searchResponse.getHits();

        SearchHit[] hitsHits = hits.getHits();

        for (SearchHit hitsHit : hitsHits) {
            String sourceAsString = hitsHit.getSourceAsString();
            System.out.println(sourceAsString);
        }

    }

    /**
     * 范围查询  查询年龄在[18，28]
     */
    @Test
    public void queryFromRang() {

        SearchResponse searchResponse = client.prepareSearch("indexsearch")
                .setTypes("mysearch")
                .setQuery(new RangeQueryBuilder("age").gt(17).lt(29))
                .get();

        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }

    }

    /**
     * 词条查询  say 中 包含熟悉的所有数据
     */
    @Test
    public void queryByTerm() {
        SearchResponse searchResponse = client.prepareSearch("indexsearch")
                .setTypes("myindex")
                .setQuery(new TermQueryBuilder("say", "熟悉"))
                .get();

        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }

    }

    /**
     * 模糊查询   Fuzziness.TWO标识英文单词最大纠正次数为2
     */
    @Test
    public void queryByFuzzy(){
        SearchResponse searchResponse = client.prepareSearch("indexsearch")
                .setTypes("mysearch")
                .setQuery(new FuzzyQueryBuilder("say", "helOL").fuzziness(Fuzziness.TWO))
                .get();

        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }


    }

    /**
     * 查询使用通配符匹配查询  * 匹配任意字符包含空串  ？ 匹配单个字符串
     */
    @Test
    public void queryByWildCard(){
        SearchResponse searchResponse = client.prepareSearch("indexsearh")
                .setTypes("search")
                .setQuery(new WildcardQueryBuilder("say", "hell*"))
                .get();

        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * 使用boolQuery实现多条件组合查询
     * 查询 age 在 18 - 28 , sex为男性 , 或者 id 在10 -24
     */
    @Test
    public void queryByBool(){

        RangeQueryBuilder ageRange = new RangeQueryBuilder("age")
                .gt(17)
                .lt(29);

        TermQueryBuilder sexTerm = QueryBuilders.termQuery("sex", "1");

        RangeQueryBuilder idRange = QueryBuilders.rangeQuery("id").gt(9).lt(25);

        SearchResponse searchResponse = client.prepareSearch("indexsearch")
                .setTypes("mysearch")
                .setQuery(QueryBuilders.boolQuery()
                        .should(idRange)
                        .must(ageRange)
                        .must(sexTerm))
                .get();


        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * 分页查询，并且按照id进行升序排列
     */
    @Test
    public void getPageIndex(){
        int pageSize = 5;
        int pageNum = 2;
        int startNum = (pageNum-1)*5;

        SearchResponse searchResponse = client.prepareSearch("indexsearch")
                .setTypes("mysearch")
                .setQuery(QueryBuilders.matchAllQuery())
                .addSort("id", SortOrder.ASC)
                .setFrom(startNum)
                .setSize(pageSize)
                .get();

        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }

    }

    /**
     * 实现查询结果高亮
     */
    @Test
    public void highLight(){
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("indexsearch")
                .setTypes("mysearch")
                .setQuery(QueryBuilders.termQuery("say", "hello"));


        HighlightBuilder builder = new HighlightBuilder();
        builder.field("say")
                .preTags("<font color = red >")
                .postTags("</font>");

        SearchResponse searchResponse = searchRequestBuilder.highlighter(builder).get();

        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            //System.out.println(hit.getSourceAsString());
            Text[] says = hit.getHighlightFields().get("say").getFragments();
            for (Text say : says) {
                System.out.println(say);
            }
        }

    }

    /**
     * 指定id更新索引
     */
    @Test
    public void updateIndex(){
        Person newPerson = new Person(5, "宋江", 88, 0, "水泊梁山", "17666666666", "wusong@itcast.com","及时雨宋江");

        UpdateResponse updateResponse = client.prepareUpdate()
                .setIndex("indexsearch")
                .setType("mysearch")
                .setId("5")
                .setDoc(JSON.toJSON(newPerson), XContentType.JSON)
                .get();

        System.out.println(updateResponse.status());
    }

    /**
     * 指定id删除数据
     */
    @Test
    public void deleteIndexData(){
        DeleteResponse deleteResponse = client.prepareDelete("indexsearch", "myindex", "14").get();

        System.out.println(deleteResponse.status());
    }

    /**
     * 按照条件进行删除
     */
    @Test
    public void deleteByQuery(){
        BulkByScrollResponse bulkByScrollResponse = DeleteByQueryAction.INSTANCE
                .newRequestBuilder(client)
                .filter(QueryBuilders.rangeQuery("id").gt(14).lt(24))
                .source("indexsearch")
                .get();

        long deleted = bulkByScrollResponse.getDeleted();

        System.out.println(deleted);

    }

    /**
     * 删除索引库
     */
    @Test
    public void deleteIndex(){
        client.admin()
                .indices()
                .prepareDelete("indexsearch")
                .execute()
                .actionGet();
    }
}
