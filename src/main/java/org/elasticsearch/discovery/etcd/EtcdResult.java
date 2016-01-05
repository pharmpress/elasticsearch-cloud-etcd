package org.elasticsearch.discovery.etcd;

import java.util.List;

public class EtcdResult {
    public String action;
    public EtcdNode node;
    public EtcdNode prevNode;
    public List<EtcdNode> nodes;
    public int errorCode = 0;
    public String message;
    public String cause;
    public int index;
}
