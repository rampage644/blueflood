package com.rackspacecloud.blueflood.inputs.handlers;

import com.rackspacecloud.blueflood.cache.ConfigTtlProvider;
import com.rackspacecloud.blueflood.inputs.formats.JSONMetricsContainer;
import com.rackspacecloud.blueflood.utils.TimeValue;
import io.netty.util.internal.StringUtil;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HttpGraphiteMetricsIngestionHandler extends HttpMetricsIngestionHandler {
    public HttpGraphiteMetricsIngestionHandler(HttpMetricsIngestionServer.Processor processor, TimeValue timeout) {
        super(processor, timeout);
    }

    @Override
    protected JSONMetricsContainer createContainer(String body, String tenantId) throws JsonParseException, JsonMappingException, IOException {
        List<JSONMetricsContainer.JSONMetric> jsonMetrics = new ArrayList<JSONMetricsContainer.JSONMetric>();
        String[] inputMetricFields = StringUtil.split(body, ' ');
        JSONMetricsContainer.JSONMetric metric = new JSONMetricsContainer.JSONMetric();
        metric.setMetricName(inputMetricFields[0]);
        metric.setMetricValue(Double.parseDouble(inputMetricFields[1]));
        metric.setCollectionTime(Long.parseLong(inputMetricFields[2]));
        metric.setTtlInSeconds((int)ConfigTtlProvider.getInstance().getConfigTTLForIngestion().toSeconds());
        jsonMetrics.add(metric);
        return new JSONMetricsContainer(tenantId, jsonMetrics);
    }
}
