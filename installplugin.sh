#!/bin/bash

~/Applications/elasticsearch/elasticsearch-1.4.2/bin/plugin  --remove elasticsearch-cloud-etcd-2.1-SNAPSHOT

sleep 2

~/Applications/elasticsearch/elasticsearch-1.4.2/bin/plugin  --url file://$HOME/Workspaces/elasticsearch-cloud-etcd/target/releases/elasticsearch-cloud-etcd-2.1-SNAPSHOT.zip  --install  elasticsearch-cloud-etcd-2.1-SNAPSHOT

sleep 2

~/Applications/elasticsearch/elasticsearch-1.4.2-A/bin/plugin  --remove elasticsearch-cloud-etcd-2.1-SNAPSHOT

sleep 2

~/Applications/elasticsearch/elasticsearch-1.4.2-A/bin/plugin  --url file://$HOME/Workspaces/elasticsearch-cloud-etcd/target/releases/elasticsearch-cloud-etcd-2.1-SNAPSHOT.zip  --install  elasticsearch-cloud-etcd-2.1-SNAPSHOT

