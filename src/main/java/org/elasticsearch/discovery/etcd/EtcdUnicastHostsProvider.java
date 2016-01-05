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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastHostsProvider;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastZenPing;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import java.net.InetSocketAddress;
import java.util.*;
import org.elasticsearch.common.inject.Inject;

public class EtcdUnicastHostsProvider extends AbstractComponent implements UnicastHostsProvider {

    private TransportService transportService;

    private EtcdClient etcdClient;

    private String etcdInstanceKey;

    private String etcdTransportkey;

    private int etcdInterval = 5;

    @Inject
    public EtcdUnicastHostsProvider(Settings settings, TransportService transportService, Transport transport) {
        super(settings);
        this.transportService = transportService;
        String etcdHost;
        String etcdKey;
        if (System.getenv().containsKey("ETCDCTL_PEERS")) {
            etcdHost = System.getenv().get("ETCDCTL_PEERS");
        } else {
            etcdHost = settings.get("cloud.etcd.host", "127.0.0.1:4001");
        }
        etcdKey = settings.get("cloud.etcd.key", "/services/elasticsearch");
        etcdInstanceKey = settings.get("cloud.etcd.instance.key", String.valueOf(System.currentTimeMillis()));
        etcdTransportkey = settings.get("cloud.etcd.transport.key", "transport");
        try {
            etcdInterval = Integer.parseInt(settings.get("cloud.etcd.interval", "5"));
        } catch (Throwable ignored) {}
        etcdClient = new EtcdClient(logger, etcdHost, etcdKey, etcdInterval);

    }

    @Override
    public List<DiscoveryNode> buildDynamicNodes() {
        List<DiscoveryNode> nodes = new ArrayList<>();
        try {
            do {
                notifyPeer();
                nodes = discoveryNodes();
            } while(waitForPeer(nodes));
            return nodes;
        } catch (Exception e) {
            logger.error("etcdService error :" + e.getMessage());
            logger.trace("etcdService error", e);
            return nodes;
        }
    }

    private void notifyPeer() throws Exception {
        String value = "";
        TransportAddress transportAddress = transportService.boundAddress().publishAddress();
        if(transportAddress instanceof InetSocketTransportAddress) {
            InetSocketAddress publishAddress = ((InetSocketTransportAddress)transportAddress).address();
            value = publishAddress.getHostString() + ":" + publishAddress.getPort();
        }
        if(!"".equals(value) ){
            EtcdResult etcdResult = etcdClient.queryEtcdKeys(etcdInstanceKey);
            String content;
            if(etcdResult != null && etcdResult.node != null && etcdResult.node.dir) {
                content = etcdClient.updateEtcdkeys(etcdInstanceKey + "/" + etcdTransportkey , value);
            } else {
                content = etcdClient.updateEtcdkeys(etcdInstanceKey, value);
            }
            logger.debug("update etcd: " + content);
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
            Thread.sleep(etcdInterval * 1000);
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
        EtcdResult result = etcdClient.queryEtcdKeys("");
        if(result != null) {
            if (result.node != null && result.node.nodes != null && !result.node.nodes.isEmpty()) {
                for (EtcdNode node : result.node.nodes) {
                    String serviceKey = node.key;
                    String id = serviceKey.substring(serviceKey.lastIndexOf("/") + 1);
                    if (node.dir && node.nodes != null) {
                        for (EtcdNode subnode : node.nodes) {
                            if ((serviceKey + "/" + etcdTransportkey).equals(subnode.key) && subnode.value != null && !subnode.value.isEmpty()) {
                                locations.addAll(createDiscoveryNode(subnode.value, id));
                            }
                        }
                    } else if(node.value != null){
                        locations.addAll(createDiscoveryNode(node.value, id));
                    }
                }
            } else if (result.errorCode != 0) {
                logger.error(String.format("Error[%d] %s : %s", result.errorCode, result.message, result.cause));
            } else {
                logger.info("Empty response from etcd");
            }
        }
        return locations;
    }
}
