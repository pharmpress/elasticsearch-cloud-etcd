package org.elasticsearch.plugin.cloud.etcd;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.Response;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;

public class EtcdServiceImpl extends
        AbstractLifecycleComponent<EtcdServiceImpl> implements EtcdService {

    public static class EtcdNode {
        public String key;
        public long createdIndex;
        public long modifiedIndex;
        public String value;
        public String expiration;
        public int ttl;
        public boolean dir;
        public List<EtcdNode> nodes;
    }


    public static class EtcdResult {
        public String action;
        public EtcdNode node;
        public EtcdNode prevNode;
        public List<EtcdNode> nodes;
        public int errorCode;
        public String message;
        public String cause;
        public int index;
    }

    private static class LocationImpl implements Location {

        private String address;
        private String id;

        public LocationImpl(String address, String id){
            this.address = address;
            this.id = id;
        }

        public String getAddress() {
            return address;
        }

        public String getId(){
            return id;
        }
    }

    private String etcdHost;

    private String etcdKey;

    @Inject
    public EtcdServiceImpl(Settings settings, SettingsFilter settingsFilter) {
        super(settings);
        etcdHost = settings.get("cloud.etcd.host","http://127.0.0.1:4001");
        etcdKey = settings.get("cloud.etcd.key","/services/elasticsearch");
    }

    @Override
    public List<Location> transports() {
        List<Location> locations = new ArrayList<>();

        ClientConfig config = new DefaultClientConfig();
        Client client = Client.create(config);
        WebResource service = client.resource(String.format("%s/v2/keys",
                etcdHost));

        ClientResponse response = service.path(etcdKey).queryParam("recursive", "true").get(
                ClientResponse.class);

        if (response.getStatus() != Response.Status.OK.getStatusCode()
                && response.getStatus() != Response.Status.NOT_FOUND.getStatusCode()) {
            logger.error("Error when fetching etcd");
        }

        String content = "";
        try {
            ObjectMapper mapper = new ObjectMapper();
            java.util.Scanner s = new java.util.Scanner(response.getEntityInputStream()).useDelimiter("\\A");
            if(s.hasNext()){
                content = s.next();
            }
            EtcdResult result = mapper.readValue(
                    content, EtcdResult.class);

            if(result.node.nodes != null && !result.node.nodes.isEmpty()) {
                for(EtcdNode node : result.node.nodes) {
                    String serviceKey = node.key;
                    String id = serviceKey.substring(serviceKey.lastIndexOf("/") + 1);
                    for( EtcdNode subnode : node.nodes) {
                        if((serviceKey + "/transport").equals(subnode.key)) {
                            if(subnode.value != null && !subnode.value.isEmpty()) {
                                locations.add(new LocationImpl(subnode.value, id));
                            }
                        }
                    }
                }
            } else {
                logger.info("Empty response from etcd");
            }
            return locations;
        } catch (Exception e) {
            logger.error(String.format("Response error from etcd with content:[%s]", content), e);
            return locations;
        }

    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }

    @Override
    protected void doStart() throws ElasticsearchException {

    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }
}