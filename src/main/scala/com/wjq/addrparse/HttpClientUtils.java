package com.wjq.addrparse;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.NoHttpResponseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.*;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HttpContext;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.CodingErrorAction;
import java.util.List;
import java.util.Map;


/**
 * HttpClient工具类
 */
public class HttpClientUtils {

    /**
     * 连接池最大连接数
     */
    private static final int MAX_TOTAL_CONNECTIONS = 4000;

    /**
     * 设置每个路由上的默认连接个数
     */
    private static final int DEFAULT_MAX_PER_ROUTE = 200;

    /**
     * 请求的请求超时时间 单位：毫秒
     */
    private static final int REQUEST_CONNECTION_TIMEOUT = 8 * 1000;

    /**
     * 请求的等待数据超时时间 单位：毫秒
     */
    private static final int REQUEST_SOCKET_TIMEOUT = 8 * 1000;

    /**
     * 请求的连接超时时间 单位：毫秒
     */
    private static final int REQUEST_CONNECTION_REQUEST_TIMEOUT = 5 * 1000;

    /**
     * 连接闲置多久后需要重新检测 单位：毫秒
     */
    private static final int VALIDATE_AFTER_IN_ACTIVITY = 2 * 1000;

    /**
     * 关闭Socket时，要么发送完所有数据，要么等待多少秒后，就关闭连接，此时socket.close()是阻塞的　单位秒
     */
    private static final int SOCKET_CONFIG_SO_LINGER = 60;

    /**
     * 接收数据的等待超时时间,即读超时时间，单位ms
     */
    private static final int SOCKET_CONFIG_SO_TIMEOUT = 5 * 1000;
    /**
     * 重试次数
     */
    private static int RETRY_COUNT = 5;
    /**
     * 声明为 static volatile,会迫使线程每次读取时作为一个全局变量读取
     */
    private static volatile CloseableHttpClient httpClient = null;


    /**
     * @param uri
     * @return String
     * @description get请求方式
     * @author: long.he01
     */
    public static String doGet(String uri) {

        StringBuilder result = new StringBuilder();
        try {
            String res = "";
            URL url = new URL(uri);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
            String line;
            while ((line = in.readLine()) != null) {
                res += line + "\n";
            }
            in.close();
            return res;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }

    /**
     * @param uri
     * @param params
     * @return string
     * @description 带map参数get请求, 此方法会将map参数拼接到连接地址上。
     */
    public static String doGet(String uri, Map<String, String> params) {

        return doGet(getGetUrlFromParams(uri, params));

    }

    /**
     * @param uri
     * @param params
     * @return String
     * @description 根据map参数拼接完整的url地址
     */
    private static String getGetUrlFromParams(String uri, Map<String, String> params) {


        List<BasicNameValuePair> resultList = FluentIterable.from(params.entrySet()).transform(
                new Function<Map.Entry<String, String>, BasicNameValuePair>() {
                    @Override
                    public BasicNameValuePair apply(Map.Entry<String, String> innerEntry) {

                        return new BasicNameValuePair(innerEntry.getKey(), innerEntry.getValue());
                    }

                }).toList();

        String paramSectionOfUrl = URLEncodedUtils.format(resultList, Consts.UTF_8);
        StringBuffer resultUrl = new StringBuffer(uri);

        if (StringUtils.isEmpty(uri)) {
            return uri;
        } else {
            if (!StringUtils.isEmpty(paramSectionOfUrl)) {
                if (uri.endsWith("?")) {
                    resultUrl.append(paramSectionOfUrl);
                } else {
                    resultUrl.append("?").append(paramSectionOfUrl);
                }
            }
            return resultUrl.toString();
        }


    }


    /**
     * @param uri
     * @param params
     * @return String
     * @description 带map参数的post请求方法
     */
    public static String doPost(String uri, Map<String, String> params) {

        String responseBody;
        HttpPost httpPost = new HttpPost(uri);
        try {
            List<NameValuePair> nvps = Lists.newArrayList();
            for (Map.Entry<String, String> entry : params.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                nvps.add(new BasicNameValuePair(key, value));
            }
            httpPost.setEntity(new UrlEncodedFormEntity(nvps, Consts.UTF_8));
            httpPost.setConfig(getRequestConfig());
            responseBody = executeRequest(httpPost);

        } catch (Exception e) {
            throw new RuntimeException("httpclient doPost方法异常 ", e);
        } finally {
            httpPost.releaseConnection();
        }

        return responseBody;

    }


    /**
     * @param uri
     * @param param
     * @param contentType 根据具体请求情况指定,比如json可以是 ContentType.APPLICATION_JSON
     * @return String
     * @description 带单string参数执行post方法
     */
    public static String doPost(String uri, String param, ContentType contentType) {

        String responseBody;
        HttpPost httpPost = new HttpPost(uri);
        try {
            StringEntity reqEntity = new StringEntity(param, contentType);
            httpPost.setEntity(reqEntity);
            httpPost.setConfig(getRequestConfig());
            responseBody = executeRequest(httpPost);

        } catch (IOException e) {
            throw new RuntimeException("httpclient doPost方法异常 ", e);
        } finally {
            httpPost.releaseConnection();
        }
        return responseBody;
    }

    /**
     * @return RequestConfig
     * @description: 获得请求配置信息
     */
    private static RequestConfig getRequestConfig() {


        RequestConfig defaultRequestConfig = RequestConfig.custom()
                //.setCookieSpec(CookieSpecs.DEFAULT)
                .setExpectContinueEnabled(true)
                //.setTargetPreferredAuthSchemes(Arrays.asList(AuthSchemes.NTLM, AuthSchemes.DIGEST))
                //.setProxyPreferredAuthSchemes(Arrays.asList(AuthSchemes.BASIC))
                .build();

        return RequestConfig.copy(defaultRequestConfig)
                .setSocketTimeout(REQUEST_CONNECTION_TIMEOUT)
                .setConnectTimeout(REQUEST_SOCKET_TIMEOUT)
                .setConnectionRequestTimeout(REQUEST_CONNECTION_REQUEST_TIMEOUT)
                .build();

    }


    /**
     * @param method
     * @return String
     * @throws IOException
     * @description 通用执行请求方法
     */
    private static String executeRequest(HttpUriRequest method) throws IOException {

        ResponseHandler<String> responseHandler = new ResponseHandler<String>() {

            @Override
            public String handleResponse(final HttpResponse response) throws IOException {

                int status = response.getStatusLine().getStatusCode();
                String result;
                if (status >= HttpStatus.SC_OK && status < HttpStatus.SC_MULTIPLE_CHOICES) {
                    HttpEntity entity = response.getEntity();
                    result = entity != null ? EntityUtils.toString(entity) : null;
                    EntityUtils.consume(entity);
                    return result;
                } else {
                    throw new ClientProtocolException("Unexpected response status: " + status);
                }
            }

        };
        String result = getHttpClientInstance().execute(method, responseHandler);

        return result;
    }


    /**
     * @return CloseableHttpClient
     * @description 单例获取httpclient实例
     */
    private static CloseableHttpClient getHttpClientInstance() {

        if (httpClient == null) {
            synchronized (CloseableHttpClient.class) {
                if (httpClient == null) {
                    httpClient = HttpClients.custom().setConnectionManager(initConfig()).setRetryHandler(getRetryHandler()).build();
                }
            }
        }
        return httpClient;

    }

    /**
     * @return HttpRequestRetryHandler
     * @description :获取重试handler
     */
    private static HttpRequestRetryHandler getRetryHandler() {

        // 请求重试处理
        return new HttpRequestRetryHandler() {
            @Override
            public boolean retryRequest(IOException exception,
                                        int executionCount, HttpContext context) {
                if (executionCount >= RETRY_COUNT) {
                    // 假设已经重试了5次，就放弃
                    return false;
                }
                if (exception instanceof NoHttpResponseException) {
                    // 假设server丢掉了连接。那么就重试
                    return true;
                }
                if (exception instanceof SSLHandshakeException) {
                    // 不要重试SSL握手异常
                    return false;
                }
                if (exception instanceof InterruptedIOException) {
                    // 超时
                    return false;
                }
                if (exception instanceof UnknownHostException) {
                    // 目标server不可达
                    return false;
                }
                if (exception instanceof ConnectTimeoutException) {
                    // 连接被拒绝
                    return false;
                }
                if (exception instanceof SSLException) {
                    // SSL握手异常
                    return false;
                }

                HttpRequest request = HttpClientContext.adapt(context).getRequest();
                // 假设请求是幂等的，就再次尝试
                return !(request instanceof HttpEntityEnclosingRequest);
            }
        };

    }


    /**
     * @return PoolingHttpClientConnectionManager
     * @description 初始化连接池等配置信息
     */
    private static PoolingHttpClientConnectionManager initConfig() {

        Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", PlainConnectionSocketFactory.INSTANCE)
                .register("https", new SSLConnectionSocketFactory(SSLContexts.createSystemDefault()))
                .build();

        PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager(socketFactoryRegistry);

        /**
         * 以下参数设置含义分别为:
         * 1 是否立即发送数据，设置为true会关闭Socket缓冲，默认为false
         * 2 是否可以在一个进程关闭Socket后，即使它还没有释放端口，其它进程还可以立即重用端口
         * 3 接收数据的等待超时时间，单位ms
         * 4 关闭Socket时，要么发送完所有数据，要么等待多少秒后，就关闭连接，此时socket.close()是阻塞的
         * 5 开启监视TCP连接是否有效
         * 其中setTcpNoDelay(true)设置是否启用Nagle算法，设置true后禁用Nagle算法，默认为false（即默认启用Nagle算法）。
         * Nagle算法试图通过减少分片的数量来节省带宽。当应用程序希望降低网络延迟并提高性能时，
         * 它们可以关闭Nagle算法，这样数据将会更早地发 送，但是增加了网络消耗。 单位为：毫秒
         */

        SocketConfig socketConfig = SocketConfig.custom()
                .setTcpNoDelay(true)
                .setSoReuseAddress(true)
                .setSoTimeout(SOCKET_CONFIG_SO_TIMEOUT)
                //.setSoLinger(SOCKET_CONFIG_SO_LINGER)
                //.setSoKeepAlive(true)
                .build();

        connManager.setDefaultSocketConfig(socketConfig);
        connManager.setValidateAfterInactivity(VALIDATE_AFTER_IN_ACTIVITY);

        ConnectionConfig connectionConfig = ConnectionConfig.custom()
                .setMalformedInputAction(CodingErrorAction.IGNORE)
                .setUnmappableInputAction(CodingErrorAction.IGNORE)
                .setCharset(Consts.UTF_8)
                .build();
        connManager.setDefaultConnectionConfig(connectionConfig);
        connManager.setDefaultMaxPerRoute(DEFAULT_MAX_PER_ROUTE);
        connManager.setMaxTotal(MAX_TOTAL_CONNECTIONS);
        return connManager;

    }

}
