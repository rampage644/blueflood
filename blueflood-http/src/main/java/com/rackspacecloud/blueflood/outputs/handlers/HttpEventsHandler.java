package com.rackspacecloud.blueflood.outputs.handlers;

import com.rackspacecloud.blueflood.http.HTTPRequestWithDecodedQueryParams;
import com.rackspacecloud.blueflood.http.HttpRequestHandler;
import com.rackspacecloud.blueflood.http.HttpResponder;
import com.rackspacecloud.blueflood.io.GenericElasticSearchIO;
import com.rackspacecloud.blueflood.io.Constants;

import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.utils.DateTimeParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.*;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        String response = "";
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            Event event = objectMapper.readValue(request.getContent().array(), Event.class);
            if (event.getWhen() == 0) {
                event.setWhen(new DateTime().getMillis() / 1000);
            }

            if (event.getWhat().equals("")) {
                throw new Exception("Event should contain at least 'what' field.");
            }
            searchIO.insert(tenantId, Arrays.asList(event.toMap()));
        }
        catch (Exception e) {
            log.error(String.format("Exception %s", e.toString()));
            response = String.format("Error: %s", e.getMessage());
        }

        sendResponse(ctx, request, response, HttpResponseStatus.OK);
    }

    private void handleGetEvent(ChannelHandlerContext ctx, HttpRequest request, String tenantId) {

        ObjectMapper objectMapper = new ObjectMapper();
        String responseBody;
        try {
            HTTPRequestWithDecodedQueryParams requestWithParams = (HTTPRequestWithDecodedQueryParams) request;
            Map<String, List<String>> params = requestWithParams.getQueryParams();

            parseDateFieldInQuery(params, "from");
            parseDateFieldInQuery(params, "until");

            List<Map<String, Object>> searchResult = searchIO.search(tenantId, params);
            responseBody = objectMapper.writeValueAsString(searchResult);
        }
        catch (Exception e) {
            log.error(String.format("Exception %s", e.toString()));
            responseBody = String.format("Error: %s", e.getMessage());
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

    private void parseDateFieldInQuery(Map<String, List<String>> params, String name) {
        if (params.containsKey(name)) {
            String fromValue = extractDateFieldFromQuery(params.get(name));
            params.put(name, Arrays.asList(fromValue));
        }
    }

    private String extractDateFieldFromQuery(List<String> value) {
        DateTime dateTime = DateTimeParser.parse(value.get(0));
        return Long.toString(dateTime.getMillis() / 1000);
    }

    private static class Event {
        private long when = 0;
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

        public long getWhen() {
            return when;
        }

        public void setWhen(long when) {
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
