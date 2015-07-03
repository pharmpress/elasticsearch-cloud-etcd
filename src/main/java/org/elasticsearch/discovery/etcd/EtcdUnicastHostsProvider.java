/*
 * (C) Copyright 2014 Nuxeo SA (http://nuxeo.com/) and others.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * Contributors:
 *     * Damien Metzler <dmetzler@nuxeo.com>
 */
package org.elasticsearch.discovery.etcd;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastHostsProvider;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastZenPing;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

public class EtcdUnicastHostsProvider extends AbstractComponent implements UnicastHostsProvider {
    public static class EtcdNode {
        public String key;
        public long createdIndex;
        public long modifiedIndex;
        public String value;
        public String expiration;
        public int ttl;
        public boolean dir = false;
        public List<EtcdNode> nodes;
    }

    public static class EtcdResult {
        public String action;
        public EtcdNode node;
        public EtcdNode prevNode;
        public List<EtcdNode> nodes;
        public int errorCode = 0;
        public String message;
        public String cause;
        public int index;
    }

    public static class Location {
        private String address;
        private String id;

        public Location(String address, String id) {
            this.address = address;
            this.id = id;
        }

        public String getAddress() {
            return address;
        }

        public String getId() {
            return id;
        }
    }

    private TransportService transportService;

    private String etcdHost;

    private String etcdKey;

    private int etcdInterval = 5;

    @Inject
    public EtcdUnicastHostsProvider(Settings settings, TransportService transportService, Transport transport) {
        super(settings);
        this.transportService = transportService;
        if (System.getenv().containsKey("ETCDCTL_PEERS")) {
            etcdHost = System.getenv().get("ETCDCTL_PEERS");
        } else {
            etcdHost = settings.get("cloud.etcd.host", "127.0.0.1:4001");
        }
        etcdKey = settings.get("cloud.etcd.key", "/services/elasticsearch");
        etcdInterval = Integer.parseInt(settings.get("cloud.etcd.interval", "5"));
    }

    @Override
    public List<DiscoveryNode> buildDynamicNodes() {
        List<DiscoveryNode> nodes = new ArrayList<>();
        try {
            do {
                nodes = discoveryNodes();
            } while(waitForPeer(nodes));
            return nodes;
        } catch (Exception e) {
            logger.error("etcdService error :" + e.getMessage());
            logger.trace("etcdService error", e);
            return nodes;
        }
    }

    private boolean waitForPeer(List<DiscoveryNode> nodes) throws InterruptedException {
        boolean peerAbsent = true;
        for(DiscoveryNode node: nodes){
            if(!transportService.boundAddress().publishAddress().equals(node.address())){
                peerAbsent = false;
            }
        }
        if(peerAbsent){
            logger.warn("Waiting " + etcdInterval + "s for peer ...");
            Thread.sleep(etcdInterval*1000);
        }
        return peerAbsent;
    }

    private List<DiscoveryNode> createDiscoveryNode(String address,String id) throws Exception {
        List<DiscoveryNode> nodes = new ArrayList<>();
        TransportAddress[] addresses = transportService.addressesFromString(address);
        for (int i = 0; (i < addresses.length && i < UnicastZenPing.LIMIT_PORTS_COUNT); i++) {
            logger.trace("adding {}, transport_address {}", address, addresses[i]);
            nodes.add(new DiscoveryNode("#cloud-" + id + "-" + i, addresses[i], Version.CURRENT));
        }
        return nodes;
    }

    public List<DiscoveryNode> discoveryNodes() throws Exception {
        List<DiscoveryNode> locations = new ArrayList<>();
        WebResource service = Client.create(new DefaultClientConfig()).resource(String.format("http://%s/v2/keys", etcdHost));
        ClientResponse response = service.path(etcdKey).queryParam("recursive", "true").get(ClientResponse.class);
        ClientResponse.Status status = response.getClientResponseStatus();

        if (status != ClientResponse.Status.OK && status != ClientResponse.Status.NOT_FOUND) {
            logger.error(String.format("Error when fetching etcd[%d]: %s", status.getStatusCode(), status.toString()));
        } else {
            String content = "";
            try {
                ObjectMapper mapper = new ObjectMapper();
                Scanner s = new Scanner(response.getEntityInputStream()).useDelimiter("\\A");
                if (s.hasNext()) {
                    content = s.next();
                }
                EtcdResult result = mapper.readValue(content, EtcdResult.class);

                if (result.node != null && result.node.nodes != null && !result.node.nodes.isEmpty()) {
                    for (EtcdNode node : result.node.nodes) {
                        String serviceKey = node.key;
                        String id = serviceKey.substring(serviceKey.lastIndexOf("/") + 1);
                        if (node.dir && node.nodes != null) {
                            for (EtcdNode subnode : node.nodes) {
                                if ((serviceKey + "/transport").equals(subnode.key) && subnode.value != null && !subnode.value.isEmpty()) {
                                    locations.addAll(createDiscoveryNode(subnode.value, id));
                                }
                            }
                        } else if(node.value != null){
                            locations.addAll(createDiscoveryNode(node.value, id));
                        }
                    }
                } else if (result.errorCode != 0) {
                    throw new Exception(String.format("Error[%d] %s : %s", result.errorCode, result.message, result.cause));
                } else {
                    logger.info("Empty response from etcd");
                }
            } catch (Exception e) {
                throw new Exception(String.format("Response error from etcd with content:[%s]", content), e);
            }
        }
        return locations;
    }
}
