package org.elasticsearch.discovery.etcd;

import java.util.List;

public class EtcdNode {
    public String key;
    public long createdIndex;
    public long modifiedIndex;
    public String value;
    public String expiration;
    public int ttl;
    public boolean dir = false;
    public List<EtcdNode> nodes;
}
