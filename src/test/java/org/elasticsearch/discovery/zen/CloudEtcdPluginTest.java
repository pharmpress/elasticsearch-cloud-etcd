package org.elasticsearch.discovery.zen;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.etcd.EtcdDiscovery;
import org.elasticsearch.discovery.zen.fd.FaultDetection;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.hamcrest.Matchers;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.*;

/**
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
@Slow
public class CloudEtcdPluginTest extends ElasticsearchIntegrationTest {

    @Test
    @Ignore
    public void testChangeRejoinOnMasterOptionIsDynamic() throws Exception {
        Settings nodeSettings = ImmutableSettings.settingsBuilder()
                .put("discovery.type", "etcd") // <-- To override the local setting if set externally
                .put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, true) //
                .put("cloud.enabled", true) //
                .build();
        String nodeName = internalCluster().startNode(nodeSettings);
        EtcdDiscovery zenDiscovery = (EtcdDiscovery) internalCluster().getInstance(Discovery.class, nodeName);
        //assertThat(zenDiscovery.isRejoinOnMasterGone(), is(true));

        client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(ImmutableSettings.builder().put(ZenDiscovery.SETTING_REJOIN_ON_MASTER_GONE, false))
                .get();

        //assertThat(zenDiscovery.isRejoinOnMasterGone(), is(false));
    }

    @Test
    @Ignore
    public void testNoShardRelocationsOccurWhenElectedMasterNodeFails() throws Exception {
        Settings defaultSettings = ImmutableSettings.builder()
                .put(FaultDetection.SETTING_PING_TIMEOUT, "1s")
                .put(FaultDetection.SETTING_PING_RETRIES, "1")
                .put("discovery.type", "etcd")
                .put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, true) //
                .put("cloud.enabled", true) //
                .build();

        Settings masterNodeSettings = ImmutableSettings.builder()
                .put("node.data", false)
                .put(defaultSettings)
                .build();
        internalCluster().startNodesAsync(2, masterNodeSettings).get();
        Settings dateNodeSettings = ImmutableSettings.builder()
                .put("node.master", false)
                .put(defaultSettings)
                .build();
        internalCluster().startNodesAsync(2, dateNodeSettings).get();
        ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForNodes("4")
                .setWaitForRelocatingShards(0)
                .get();
        assertThat(clusterHealthResponse.isTimedOut(), is(false));

        createIndex("test");
        ensureSearchable("test");
        RecoveryResponse r = client().admin().indices().prepareRecoveries("test").get();
        int numRecoveriesBeforeNewMaster = r.shardResponses().get("test").size();

        final String oldMaster = internalCluster().getMasterName();
        internalCluster().stopCurrentMasterNode();
        assertBusy(new Runnable() {
            @Override
            public void run() {
                String current = internalCluster().getMasterName();
                assertThat(current, notNullValue());
                assertThat(current, not(equalTo(oldMaster)));
            }
        });
        ensureSearchable("test");

        r = client().admin().indices().prepareRecoveries("test").get();
        int numRecoveriesAfterNewMaster = r.shardResponses().get("test").size();
        assertThat(numRecoveriesAfterNewMaster, equalTo(numRecoveriesBeforeNewMaster));
    }

    @Test
    @Ignore
    @TestLogging(value = "action.admin.cluster.health:TRACE")
    public void testNodeFailuresAreProcessedOnce() throws ExecutionException, InterruptedException, IOException {
        Settings defaultSettings = ImmutableSettings.builder()
                .put(FaultDetection.SETTING_PING_TIMEOUT, "1s")
                .put(FaultDetection.SETTING_PING_RETRIES, "1")
                .put("discovery.type", "etcd")
                .build();

        Settings masterNodeSettings = ImmutableSettings.builder()
                .put("node.data", false)
                .put(defaultSettings)
                .build();
        String master = internalCluster().startNode(masterNodeSettings);
        Settings dateNodeSettings = ImmutableSettings.builder()
                .put("node.master", false)
                .put(defaultSettings)
                .build();
        internalCluster().startNodesAsync(2, dateNodeSettings).get();
        client().admin().cluster().prepareHealth().setWaitForNodes("3").get();

        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, master);
        final ArrayList<ClusterState> statesFound = new ArrayList<>();
        final CountDownLatch nodesStopped = new CountDownLatch(1);
        clusterService.add(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                statesFound.add(event.state());
                try {
                    // block until both nodes have stopped to accumulate node failures
                    nodesStopped.await();
                } catch (InterruptedException e) {
                    //meh
                }
            }
        });

        internalCluster().stopRandomNonMasterNode();
        internalCluster().stopRandomNonMasterNode();
        nodesStopped.countDown();

        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).get(); // wait for all to be processed
        assertThat(statesFound, Matchers.hasSize(2));
    }
}
