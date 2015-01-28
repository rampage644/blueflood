package com.rackspacecloud.blueflood.inputs.handlers;


import com.rackspacecloud.blueflood.http.HTTPRequestWithDecodedQueryParams;
import com.rackspacecloud.blueflood.io.GenericElasticSearchIO;
import com.rackspacecloud.blueflood.outputs.handlers.HttpEventsHandler;
import junit.framework.Assert;
import org.codehaus.jackson.map.ObjectMapper;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.*;

public class HttpEventsHandlerTest {
    private GenericElasticSearchIO searchIO;
    private HttpEventsHandler handler;
    private ChannelHandlerContext context;
    private Channel channel;
    private static final String TENANT = "tenant";

    public HttpEventsHandlerTest() {
        searchIO = mock(GenericElasticSearchIO.class);
        handler = new HttpEventsHandler(searchIO);
        channel = mock(Channel.class);
        context = mock(ChannelHandlerContext.class);
        when(context.getChannel()).thenReturn(channel);
        when(channel.write(anyString())).thenReturn(new SucceededChannelFuture(channel));
    }

    private Map<String, Object> createRandomEvent() {
        Map<String, Object> event = new HashMap<String, Object>();
        event.put("what", "1");
        event.put("when", "2");
        event.put("data", "3");
        event.put("tags", "4");
        return  event;
    }

    private HttpRequest createPutOneEventRequest(Map<String, Object> event) throws IOException {
        List<Map<String, Object>> events = new ArrayList<Map<String, Object>>();
        events.add(event);

        final String requestBody = new ObjectMapper().writeValueAsString(events.get(0));
        return createRequest(HttpMethod.POST, "", requestBody);
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
    public void testElasticSearchInsertCalledWhenPut() {
        try {
            List<Map<String, Object>> events = new ArrayList<Map<String, Object>>();
            Map<String, Object> event = createRandomEvent();
            events.add(event);
            handler.handle(context, createPutOneEventRequest(event));
            verify(searchIO).insert(TENANT, events);
        }
        catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testElasticSearchSearchCalledWhenGet() {
        testQuery("", new HashMap<String, List<String>>());
    }

    private void testQuery(String query, Map<String, List<String>> params) {
        handler.handle(context, createGetRequest(query));
        try {
            verify(searchIO).search(TENANT, params);
        }
        catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test public void testMalformedEventPut() {
        Assert.fail("Not implemented");
    }

    @Test public void testQueryParametersParse() {
        Assert.fail("Not implemented");
    }

    @Test public void testMinimumEventPut() {
        Assert.fail("Not implemented");
    }

    @Test public void testApplyingCurrentTimeWhenEmpty() {
        Assert.fail("Not implemented");
    }
}
