package org.elasticsearch.discovery.etcd;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import org.elasticsearch.common.logging.ESLogger;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Scanner;

public class EtcdClient {
    private final ESLogger logger;
    private final String etcdHost;
    private final String etcdKey;
    private final int etcdInterval;

    public EtcdClient(ESLogger logger, String etcdHost, String etcdKey, int etcdInterval) {
        this.logger = logger;
        this.etcdHost = etcdHost;
        this.etcdKey = etcdKey;
        this.etcdInterval = etcdInterval;
    }

    public String updateEtcdkeys(String key, String value) {
        WebResource service = Client.create(new DefaultClientConfig()).resource(String.format("http://%s/v2/keys", etcdHost));
        ClientResponse response = service.path(etcdKey + "/" + key).queryParam("value", value).queryParam("ttl", evaluateTimeToLive()).put(ClientResponse.class);
        ClientResponse.Status status = response.getClientResponseStatus();
        InputStream responseInputStream = response.getEntityInputStream();
        String content = "";
        if (status != ClientResponse.Status.OK && status != ClientResponse.Status.CREATED && status != ClientResponse.Status.NOT_FOUND) {
            logger.error(String.format("Error when fetching etcd[%d]: %s", status.getStatusCode(), status.toString()));
        } else {
            try {
                Scanner s = new Scanner(responseInputStream).useDelimiter("\\A");
                if (s.hasNext()) {
                    content = s.next();
                }
            } finally {
                try {
                    responseInputStream.close();
                } catch (Throwable ignored){}
            }
        }
        return content;
    }

    public EtcdResult queryEtcdKeys(String key) {
        WebResource service = Client.create(new DefaultClientConfig()).resource(String.format("http://%s/v2/keys", etcdHost));
        String path = etcdKey;
        if(key != null && !"".equals(key)) {
            path += "/" + key;
        }
        ClientResponse response = service.path(path).queryParam("recursive", "true").get(ClientResponse.class);
        ClientResponse.Status status = response.getClientResponseStatus();
        InputStream responseInputStream = response.getEntityInputStream();
        String content = "";
        if (status != ClientResponse.Status.OK && status != ClientResponse.Status.NOT_FOUND) {
            logger.error(String.format("Error when fetching etcd[%d]: %s", status.getStatusCode(), status.toString()));
        } else {
            try {
                Scanner s = new Scanner(responseInputStream).useDelimiter("\\A");
                if (s.hasNext()) {
                    content = s.next();
                    ObjectMapper mapper = new ObjectMapper();
                    return mapper.readValue(content, EtcdResult.class);
                }
            } catch (JsonParseException e) {
                logger.error(String.format("Response error from etcd with content:[%s]", content), e);
            } catch (JsonMappingException e) {
                logger.error(String.format("Response error from etcd with content:[%s]", content), e);
            } catch (IOException e) {
                logger.error(String.format("Response error from etcd with content:[%s]", content), e);
            } finally {
                try {
                    responseInputStream.close();
                } catch (Throwable ignored){}
            }
        }
        return null;
    }

    private String evaluateTimeToLive() {
        return new BigDecimal(etcdInterval*120).divide(new BigDecimal(100), 0, RoundingMode.UP).toString();
    }

}
