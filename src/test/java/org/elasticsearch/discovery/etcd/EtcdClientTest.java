package org.elasticsearch.discovery.etcd;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.slf4j.Slf4jESLoggerFactory;
import org.junit.Ignore;
import org.junit.Test;

public class EtcdClientTest {
    String etcdHost = "127.0.0.1:4001";
    String etcdKey = "/services/elasticsearch";
    int etcdInterval  = 5;
    ESLogger logger = Slf4jESLoggerFactory.getLogger(EtcdClientTest.class.getName());

    @Test
    @Ignore
    public void testPut() {
        EtcdClient etcdClient = new EtcdClient(logger, etcdHost, etcdKey, etcdInterval);
        System.out.println(etcdClient.updateEtcdkeys("1", "test"));
    }
}
