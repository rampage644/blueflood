package com.rackspacecloud.blueflood.outputs.handlers;

import com.rackspacecloud.blueflood.http.HTTPRequestWithDecodedQueryParams;
import com.rackspacecloud.blueflood.http.HttpRequestHandler;
import com.rackspacecloud.blueflood.http.HttpResponder;
import com.rackspacecloud.blueflood.io.GenericElasticSearchIO;
import com.rackspacecloud.blueflood.io.Constants;

import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class HttpEventsHandler implements HttpRequestHandler {
    private static final Logger log = LoggerFactory.getLogger(HttpEventsHandler.class);
    private GenericElasticSearchIO searchIO;

    public HttpEventsHandler() {
        loadEventModule();
    }
    public HttpEventsHandler(GenericElasticSearchIO searchIO) {
        this.searchIO = searchIO;
    }

    private void loadEventModule() {
        List<String> modules = Configuration.getInstance().getListProperty(CoreConfig.EVENTS_MODULES);

        if (!modules.isEmpty() && modules.size() != 1) {
            throw new RuntimeException("Cannot load query service with more than one event module");
        }

        ClassLoader classLoader = GenericElasticSearchIO.class.getClassLoader();
        for (String module : modules) {
            log.info("Loading metric event module " + module);
            try {
                Class discoveryClass = classLoader.loadClass(module);
                this.searchIO = (GenericElasticSearchIO) discoveryClass.newInstance();
                log.info("Registering metric event module " + module);
            } catch (InstantiationException e) {
                log.error("Unable to create instance of metric event class for: " + module, e);
            } catch (IllegalAccessException e) {
                log.error("Error starting metric event module: " + module, e);
            } catch (ClassNotFoundException e) {
                log.error("Unable to locate metric event module: " + module, e);
            } catch (RuntimeException e) {
                log.error("Error starting metric event module: " + module, e);
            } catch (Throwable e) {
                log.error("Error starting metric event module: " + module, e);
            }
        }
    }

    @Override
    public void handle(ChannelHandlerContext ctx, HttpRequest request) {
        final String tenantId = request.getHeader("tenantId");

        if (request.getMethod() == HttpMethod.GET) {
            handleGetEvent(ctx, request, tenantId);
        } else if (request.getMethod() == HttpMethod.POST) {
            handlePutEvent(ctx, request, tenantId);
        }


    }

    private void handlePutEvent(ChannelHandlerContext ctx, HttpRequest request, String tenantId) {
        ObjectMapper objectMapper = new ObjectMapper();
        String when = null;
        String what = null;
        String data = null;
        String tags = null;
        try {
            JsonNode node = objectMapper.readTree(request.getContent().array());
            when = node.get("when").asText();
            what = node.get("what").asText();
            data = node.get("data").asText();
            tags = node.get("tags").asText();

            HashMap<String, Object> userData = new HashMap<String, Object>();
            userData.put("when", when);
            userData.put("what", what);
            userData.put("data", data);
            userData.put("tags", tags);

            List<Map<String, Object>> events = new ArrayList<Map<String, Object>>();
            events.add(userData);
            searchIO.insert(tenantId, events);
        }
        catch (Exception e) {
            log.error(String.format("Exception %s", e.toString()));
        }

        sendResponse(ctx, request, String.format("put event for tenant: %s when: %s what: %s data: %s tags: %s",
                        tenantId, when, what, data, tags), HttpResponseStatus.OK);
    }

    private void handleGetEvent(ChannelHandlerContext ctx, HttpRequest request, String tenantId) {

        ObjectMapper objectMapper = new ObjectMapper();
        String responseBody = null;
        try {
            HTTPRequestWithDecodedQueryParams requestWithParams = (HTTPRequestWithDecodedQueryParams) request;

            List<Map<String, Object>> searchResult = searchIO.search(tenantId, requestWithParams.getQueryParams());
            responseBody = objectMapper.writeValueAsString(searchResult);
        }
        catch (Exception e) {
            log.error(String.format("Exception %s", e.toString()));
        }

        sendResponse(ctx, request, responseBody, HttpResponseStatus.OK);
    }

    private void sendResponse(ChannelHandlerContext channel, HttpRequest request, String messageBody,
                              HttpResponseStatus status) {
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);
        if (messageBody != null && !messageBody.isEmpty()) {
            response.setContent(ChannelBuffers.copiedBuffer(messageBody, Constants.DEFAULT_CHARSET));
        }
        HttpResponder.respond(channel, request, response);
    }

}
