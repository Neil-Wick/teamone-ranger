/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ranger.elasticsearch;

import static org.apache.ranger.audit.destination.ElasticSearchAuditDestination.CONFIG_PREFIX;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import org.apache.log4j.Logger;
import org.apache.ranger.audit.destination.ElasticSearchAuditDestination;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.common.PropertiesUtil;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.view.VXAccessAuditList;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Ignore;
import org.junit.Test;

import javax.ws.rs.core.MediaType;

public class ElasticSearchAccessAuditsServiceTest {
    private static final Logger LOGGER = Logger.getLogger(ElasticSearchAccessAuditsServiceTest.class);

    @Test
    @Ignore // For manual execution only
    public void testQuery() {
        ElasticSearchAccessAuditsService elasticSearchAccessAuditsService = new ElasticSearchAccessAuditsService();
        Map<String, String> properties = PropertiesUtil.getPropertiesMap();
        properties.put("ranger.audit.elasticsearch.urls", "192.168.60.229,192.168.60.230,192.168.60.231");
        properties.put("ranger.audit.elasticsearch.user", "elastic");
        properties.put("ranger.audit.elasticsearch.password", "06xXSM9dWb2g$_(3h_rKrzs?u");
        properties.put("ranger.audit.elasticsearch.port", "9200");
        properties.put("ranger.audit.elasticsearch.index", "pro-super-order-discount-tunefab-jp");
        elasticSearchAccessAuditsService.elasticSearchMgr = new ElasticSearchMgr();
        elasticSearchAccessAuditsService.elasticSearchUtil = new ElasticSearchUtil();
        elasticSearchAccessAuditsService.elasticSearchUtil.stringUtil = new StringUtil();
        elasticSearchAccessAuditsService.setRestErrorUtil(new RESTErrorUtil());
        LOGGER.info("Running searchXAccessAudits:");
        VXAccessAuditList vxAccessAuditList = elasticSearchAccessAuditsService.searchXAccessAudits(getSearchCriteria());
        LOGGER.info(String.format("searchXAccessAudits results (%d items):", vxAccessAuditList.getListSize()));
        ObjectWriter writer = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).writer();
        vxAccessAuditList.getVXAccessAudits().forEach(x -> {
            try {
                LOGGER.warn(writer.writeValueAsString(x));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        });
    }

    private SearchCriteria getSearchCriteria() {
        SearchCriteria searchCriteria = new SearchCriteria();
        searchCriteria.setDistinct(false);
        searchCriteria.setGetChildren(false);
        searchCriteria.setGetCount(true);
        searchCriteria.setMaxRows(25);
        searchCriteria.setOwnerId(null);
        searchCriteria.setSortBy("eventTime");
        searchCriteria.setSortType("desc");
        searchCriteria.setStartIndex(0);
        Calendar calendar = Calendar.getInstance();
        calendar.set(2019, 11, 13);
        searchCriteria.getParamList().put("startDate", calendar.getTime());
        searchCriteria.getParamList().put("-repoType", 7);
        searchCriteria.getParamList().put("-requestUser", new ArrayList<>());
        searchCriteria.getParamList().put("requestUser", new ArrayList<>());
        searchCriteria.getParamList().put("zoneName", new ArrayList<>());
        return searchCriteria;
    }

    @Test
    @Ignore // For manual execution only
    public void testWrite() throws IOException {
        ElasticSearchAuditDestination elasticSearchAuditDestination = new ElasticSearchAuditDestination();
        Properties properties = new Properties();
        properties.put(CONFIG_PREFIX + "." + ElasticSearchAuditDestination.CONFIG_URLS, "192.168.60.229,192.168.60.230,192.168.60.231");
        properties.put(CONFIG_PREFIX + "." + ElasticSearchAuditDestination.CONFIG_USER, "elastic");
        properties.put(CONFIG_PREFIX + "." + ElasticSearchAuditDestination.CONFIG_PORT, "9200");
        properties.put(CONFIG_PREFIX + "." + ElasticSearchAuditDestination.CONFIG_PWRD, "06xXSM9dWb2g$_(3h_rKrzs?u");
        properties.put(CONFIG_PREFIX + "." + ElasticSearchAuditDestination.CONFIG_INDEX, "prod_ranger_audit");
        elasticSearchAuditDestination.init(properties, CONFIG_PREFIX);
//        String bulkRequest = elasticSearchAuditDestination.getBulkRequest();
//        LOGGER.warn(bulkRequest);

//        RestClientBuilder restClientBuilder =
//                elasticSearchAuditDestination.getRestClientBuilder("192.168.60.229,192.168.60.230,192.168.60.231", "http", "elastic", "06xXSM9dWb2g$_(3h_rKrzs?u", 9200);
//
//        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);
//        boolean exits = restHighLevelClient.indices().open(new OpenIndexRequest("test_ranger_audit"), RequestOptions.DEFAULT).isShardsAcknowledged();
//        LOGGER.warn("exists--------" + exits);
        assert elasticSearchAuditDestination.log(Arrays.asList(getAuthzAuditEvent()));
    }

    @Test
    @Ignore // For manual execution only
    public void testRest() throws IOException {

        Client client = new Client();
        client.addFilter(new HTTPBasicAuthFilter("elastic", "06xXSM9dWb2g$_(3h_rKrzs?u"));

        ClientResponse response = client.resource("http://192.168.60.229:9200").accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
        LOGGER.warn("client----"+client);
        LOGGER.warn("ResponseStatus----" + response.getStatus());
    }

    private AuthzAuditEvent getAuthzAuditEvent() {
        AuthzAuditEvent event = new AuthzAuditEvent();
        event.setAccessResult((short) 1);
        event.setAccessType("");
        event.setAclEnforcer("");
        event.setAction("");
        event.setAdditionalInfo("");
        event.setAgentHostname("");
        event.setAgentId("");
        event.setClientIP("");
        event.setClusterName("");
        event.setClientType("");
        event.setEventCount(1);
        event.setEventDurationMS(1);
        event.setEventId("");
        event.setEventTime(new Date());
        event.setLogType("");
        event.setPolicyId(1);
        event.setPolicyVersion(1l);
        event.setRepositoryName("");
        event.setRequestData("");
        event.setRepositoryName("");
        event.setRepositoryType(1);
        event.setResourcePath("");
        event.setResultReason("");
        event.setSeqNum(1);
        event.setSessionId("");
        event.setTags(new HashSet<>());
        event.setUser("");
        event.setZoneName("");
        return event;
    }
}
