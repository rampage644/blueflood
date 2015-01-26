package com.rackspacecloud.blueflood.io;

import com.rackspacecloud.blueflood.service.ElasticClientManager;
import com.rackspacecloud.blueflood.service.RemoteElasticSearchServer;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

public class EventElasticSearchIO implements GenericElasticSearchIO {
    public static final String EVENT_INDEX = "events";
    private static final String ES_TYPE = "graphite_event";
    private final Client client;


    static enum ESFieldLabel {
        when,
        what,
        data,
        tags,
        tenantid
    }

    public EventElasticSearchIO() {
        this(RemoteElasticSearchServer.getInstance());
    }
    public EventElasticSearchIO(Client client) {
        this.client = client;
    }
    public EventElasticSearchIO(ElasticClientManager manager) {
        this(manager.getClient());
    }

    @Override
    public void insert(String tenant, List<Map<String, Object>> events) throws Exception {
        for (Map<String, Object> event : events) {
            event.put(ESFieldLabel.tenantid.toString(), tenant);
            client.prepareIndex(EVENT_INDEX, ES_TYPE)
                    .setSource(event)
                    .setRouting(tenant)
                    .execute()
                    .actionGet();
        }
    }

    @Override
    public List<Map<String, Object>> search(String tenant, String query) throws Exception {
        BoolQueryBuilder qb = boolQuery()
                .must(termQuery(ESFieldLabel.tenantid.toString(), tenant));
        SearchResponse response = client.prepareSearch(EVENT_INDEX)
                .setRouting(tenant)
                .setSize(100000)
                .setVersion(true)
                .setQuery(qb)
                .execute()
                .actionGet();

        List<Map<String, Object>> events = new ArrayList<Map<String, Object>>();
        for (SearchHit hit : response.getHits().getHits()) {
            events.add(hit.getSource());
        }

        return events;
    }
}
