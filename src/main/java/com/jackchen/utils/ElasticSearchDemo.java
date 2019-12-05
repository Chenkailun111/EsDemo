package com.jackchen.utils;

import java.util.HashMap;
import java.util.Map;

public class ElasticSearchDemo {

    public static void main(String[] args) throws Exception{
        RestClientUtils client = new RestClientUtils();
        /*String jsonString="{\n" +
                "  \"name\":\"dior chengyi\",\n" +
                "  \"desc\":\"shishang gaodang\",\n" +
                "  \"price\":7000,\n" +
                "  \"producer\":\"dior producer\",\n" +
                "  \"tags\":[\"shishang\",\"shechi\"]\n" +
                "}";
        String jsonString1="{\n" +
                "  \"name\":\"hailanzhijia chengyi\",\n" +
                "  \"desc\":\"shangwu xiuxian\",\n" +
                "  \"price\":200,\n" +
                "  \"producer\":\"hailanzhijia producer\",\n" +
                "  \"tags\":[\"xiuxian\"]\n" +
                "}";
        String jsonString2="{\n" +
                "  \"name\":\"kama chengyi\",\n" +
                "  \"desc\":\"shangwu xiuxian\",\n" +
                "  \"price\":300,\n" +
                "  \"producer\":\"kama producer\",\n" +
                "  \"tags\":[\"shishang\"]\n" +
                "}";
        client.index("lyh_index","user","1",jsonString);
        client.index("lyh_index","user","2",jsonString1);
        client.index("lyh_index","user","3",jsonString2);*/

        //client.get("lyh_index","user","3");

        /*boolean exists = client.exists("lyh_index", "user", "1");
        boolean exists1 = client.exists("lyh_index", "user", "4");
        System.out.println(exists+"\t"+exists1);*/

        //client.delete("lyh_index", "user", "3");

        /*String updateJson="{\"price2\":50000}";
        client.update("lyh_index", "user", "2",updateJson);*/

        //client.multiGet("lyh_index", "user","1","2","3");

        String json="{\n" +
                "  \"query\": {\n" +
                "    \"match\": {\n" +
                "      \"{{name}}\": \"{{chengyi}}\"\n" +
                "    }\n" +
                "  }, \n" +
                "  \"aggs\": {\n" +
                "    \"{{group_by_tags}}\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"{{tags}}\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Map<String, Object> map = new HashMap<>();
        map.put("name","name");
        map.put("chengyi","chengyi");
        map.put("group_by_tags","group_by_tags");
        map.put("tags","tags");

        client.searchTemplate("lyh_index",json,map);

        //client.bulk();
        client.closeClient();
    }
}