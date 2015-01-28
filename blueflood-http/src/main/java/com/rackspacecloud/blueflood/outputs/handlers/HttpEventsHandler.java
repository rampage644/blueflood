package com.rackspacecloud.blueflood.outputs.handlers;

import clojure.lang.Obj;
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
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;


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
        try {
            Event event = objectMapper.readValue(request.getContent().array(), Event.class);
            if (event.getWhen().equals("")) {
                DateTimeFormatter formatter = ISODateTimeFormat.dateTimeNoMillis();
                event.setWhen(formatter.print(new DateTime().getMillis()));
            }
            searchIO.insert(tenantId, Arrays.asList(event.toMap()));
        }
        catch (Exception e) {
            log.error(String.format("Exception %s", e.toString()));
        }

        sendResponse(ctx, request, String.format(""), HttpResponseStatus.OK);
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

    private static class Event {
        private String when = "";
        private String what = "";
        private String data = "";
        private String tags = "";


        public Map<String, Object> toMap() {
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("when", getWhen());
            map.put("what", getWhat());
            map.put("data", getData());
            map.put("tags", getTags());
            return map;
        }

        public String getWhen() {
            return when;
        }

        public void setWhen(String when) {
            this.when = when;
        }

        public String getWhat() {
            return what;
        }

        public void setWhat(String what) {
            this.what = what;
        }

        public String getData() {
            return data;
        }

        public void setData(String data) {
            this.data = data;
        }

        public String getTags() {
            return tags;
        }

        public void setTags(String tags) {
            this.tags = tags;
        }
    }

}
