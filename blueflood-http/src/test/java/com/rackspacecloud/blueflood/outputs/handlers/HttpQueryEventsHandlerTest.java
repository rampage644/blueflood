package com.rackspacecloud.blueflood.outputs.handlers;

import com.rackspacecloud.blueflood.http.HTTPRequestWithDecodedQueryParams;
import com.rackspacecloud.blueflood.io.GenericElasticSearchIO;
import junit.framework.Assert;
import org.codehaus.jackson.map.ObjectMapper;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;
import org.joda.time.DateTime;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

import static org.mockito.Mockito.*;

public class HttpQueryEventsHandlerTest {

    private GenericElasticSearchIO searchIO;
    private HttpQueryEventsHandler handler;
    private ChannelHandlerContext context;
    private Channel channel;
    private static final String TENANT = "tenant";

    public HttpQueryEventsHandlerTest() {
        searchIO = mock(GenericElasticSearchIO.class);
        handler = new HttpQueryEventsHandler(searchIO);
        channel = mock(Channel.class);
        context = mock(ChannelHandlerContext.class);
        when(context.getChannel()).thenReturn(channel);
        when(channel.write(anyString())).thenReturn(new SucceededChannelFuture(channel));
    }

    private Map<String, Object> createRandomEvent() {
        Map<String, Object> event = new HashMap<String, Object>();
        event.put("what", "1");
        event.put("when", (long)2);
        event.put("data", "3");
        event.put("tags", "4");
        return  event;
    }

    private HttpRequest createGetRequest(String uri) {
        return createRequest(HttpMethod.GET, uri, "");
    }

    private HttpRequest createRequest(HttpMethod method, String uri, String requestBody) {
        DefaultHttpRequest rawRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, method, "/v2.0/" + TENANT + "/events/" + uri);
        rawRequest.setHeader("tenantId", TENANT);
        if (!requestBody.equals(""))
            rawRequest.setContent(ChannelBuffers.copiedBuffer(requestBody.getBytes()));
        return HTTPRequestWithDecodedQueryParams.createHttpRequestWithDecodedQueryParams(rawRequest);
    }


    @Test
    public void testElasticSearchSearchCalledWhenGet() throws Exception {
        testQuery("", new HashMap<String, List<String>>());
    }

    private void testQuery(String query, Map<String, List<String>> params) throws Exception {
        handler.handle(context, createGetRequest(query));
        verify(searchIO).search(TENANT, params);
    }


    @Test public void testQueryParametersParse() throws Exception {
        Map<String, List<String>> params = new HashMap<String, List<String>>();
        params.put("until", Arrays.asList(nowTimestamp()));
        testQuery("?until=now", params);

        params.clear();
        params.put("until", Arrays.asList(nowTimestamp()));
        params.put("from", Arrays.asList("1422828000"));
        testQuery("?until=now&from=1422828000", params);

        params.clear();
        params.put("tags", Arrays.asList("event"));
        testQuery("?tags=event", params);
    }

    @Test
    public void testDateQueryParamProcessing() throws Exception {
        Map<String, List<String>> params = new HashMap<String, List<String>>();

        params.clear();
        params.put("until", Arrays.asList(nowTimestamp()));
        params.put("from", Arrays.asList(Long.toString(convertDateTimeToTimestamp(new DateTime(2014, 12, 30, 0, 0, 0, 0)))));
        testQuery("?until=now&from=00:00_2014_12_30", params);
    }


    private long convertDateTimeToTimestamp(DateTime date) {
        return date.getMillis() / 1000;
    }

    private String nowTimestamp() {
        return Long.toString(convertDateTimeToTimestamp(new DateTime().withSecondOfMinute(0).withMillisOfSecond(0)));
    }
}
