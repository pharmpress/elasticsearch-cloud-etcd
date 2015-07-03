"C:\apps\elasticsearch\elasticsearch-1.4.2\bin\plugin.bat"  --remove elasticsearch-cloud-etcd-1.0-SNAPSHOT

PAUSE

"C:\apps\elasticsearch\elasticsearch-1.4.2\bin\plugin.bat"  --url file:///workspace/elasticsearch-cloud-etcd/target/releases/elasticsearch-cloud-etcd-1.0-SNAPSHOT.zip  --install  elasticsearch-cloud-etcd-1.0-SNAPSHOT

PAUSE

"C:\apps\elasticsearch\elasticsearch-1.4.2_2\bin\plugin.bat"  --remove elasticsearch-cloud-etcd-1.0-SNAPSHOT

PAUSE

"C:\apps\elasticsearch\elasticsearch-1.4.2_2\bin\plugin.bat"  --url file:///workspace/elasticsearch-cloud-etcd/target/releases/elasticsearch-cloud-etcd-1.0-SNAPSHOT.zip  --install  elasticsearch-cloud-etcd-1.0-SNAPSHOT

