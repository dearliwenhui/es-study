package com.ab.core.utils;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

/**
 * @description:
 * @version: 0.0.1
 * @author: liwenhui
 * @createTime: 2022-11-26 22:33
 **/
public class HttpClientUtil {



    public static String doGet(String url) throws IOException {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(url);
        try (CloseableHttpResponse response = httpclient.execute(httpGet);
        ) {
            return EntityUtils.toString(response.getEntity());
        }
    }




}
