Etcd Cloud Plugin for etcd
==========================

The etcd Cloud plugin allow to use etcd for the unicast discovery mechanism.


The purpose of that plugin is to discover the transport adresses of the other Elasticsearch nodes in a cluster with etcd access.



# How it works

Let's say you have this configuration stored in etcd :

	/services/<<myclustername>>/1/transport: 127.0.0.1:9300
	/services/<<myclustername>>/2/transport: 127.0.0.1:9301
	/services/<<myclustername>>/3/transport: 127.0.0.1:9302

or :

	/services/<<myclustername>>/1: 127.0.0.1:9300
	/services/<<myclustername>>/2: 127.0.0.1:9301
	/services/<<myclustername>>/3: 127.0.0.1:9302

With this elasticsearch configuration : 

```yaml
  discovery.type: etcd
  cloud.enabled: true
  cloud.etcd.host: localhost:4001
  cloud.etcd.key: /services/<<myclustername>>
  
```

Then elasticsearch will use unicast discovery and try to connect to the other nodes using the transport addresses set in etcd. 



