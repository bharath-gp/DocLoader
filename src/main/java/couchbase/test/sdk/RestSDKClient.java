package couchbase.test.sdk;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Collection;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.util.concurrent.Semaphore;


public class RestSDKClient {
    static Semaphore mutex = new Semaphore(1);
    static Logger logger = LogManager.getLogger(RestSDKClient.class);
    public Server master;
    public String bucket;
    public String scope;
    public String collection;
    public String SDKServer;
    public String SDKServerLocation;

    public RestSDKClient(Server master, String bucket, String scope, String collection, String SDKServer, String SDKServerLocation) {
        super();
        this.master = master;
        this.bucket = bucket;
        this.scope = scope;
        this.collection = collection;
        if(!SDKServer.endsWith("/")) {
            SDKServer = SDKServer + "/";
        }
        this.SDKServer = SDKServer;
        this.SDKServerLocation = SDKServerLocation;
    }

    public RestSDKClient() {
        super();
    }

    public Object postRequest(String api, Object postBody, Class<?> returnClass ){
        try {
            for(int i=0; i < 5; i++) {
                if (mutex.availablePermits() == 0) {
                    logger.info("SDK Server seems to be down and another thread is trying to revive the server. Waiting for 5 secs to try again.");
                    Thread.sleep(5000);
                }
                else {
                    break;
                }
                if(i == 5){
                    logger.error("Couldn't start the server");
                    return null;
                }
            }
            ClientConfig clientConfig = new DefaultClientConfig();
            clientConfig.getClasses().add(JacksonJsonProvider.class);
            Client client = Client.create(clientConfig);
            WebResource webResource = client.resource(UriBuilder.fromUri(this.SDKServer + api).build());
            ObjectMapper mapper = new ObjectMapper();
            mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
            String test = mapper.writeValueAsString(postBody);
            ClientResponse clientResponse = webResource.type(MediaType.APPLICATION_JSON_TYPE).accept(MediaType.APPLICATION_JSON_TYPE).post(ClientResponse.class, test);
            return clientResponse.getEntity(returnClass);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (ClientHandlerException e){
            if(e.getCause().getClass() == ConnectException.class) {
                this.startSDKServer();
                return this.postRequest(api, postBody, returnClass);
            }
            else {
                throw e;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //ClientResponse clientResponse = webResource.type(MediaType.APPLICATION_JSON_TYPE).accept(MediaType.APPLICATION_JSON_TYPE).post(ClientResponse.class, postBody);
        return null;
    }

    public void initialiseSDK() {
        String api = "initialiseSDK/" + this.bucket + "/" + this.scope + "/" + this.collection;
        boolean status = (boolean) this.postRequest(api, this.master, Boolean.class);
        if(status){
            logger.info("Connected to the cluster");
        }
        else {
            logger.error("Not connected to cluster");
        }
    }

    public void connectCluster(){
        String api = "connectServer";
        boolean status = (boolean) this.postRequest(api, this.master, Boolean.class);
        if(status){
            logger.info("Connected to the cluster");
        }
        else {
            logger.error("Not connected to cluster");
        }
    }

    public  void disconnectCluster() {
        String api = "disconnectCluster";
        boolean status = (boolean) this.postRequest(api, this.master, Boolean.class);
        if(status){
            logger.info("Disconnected the cluster");
        }
        else {
            logger.error("Not disconnected the cluster");
        }
    }

    public Bucket connectBucket(String bucket){
        String api = "connectBucket/" + bucket;
        Bucket buck = (Bucket) this.postRequest(api, this.master, Bucket.class);
        return buck;
    }

    public Collection selectCollection(String bucket, String scope, String collection) {
        String api = "selectCollection/" + bucket + "/" + scope + "/" + collection;
        Collection coll = (Collection) this.postRequest(api, this.master, Collection.class);
        return coll;
    }

    public void startSDKServer(){
        if(mutex.tryAcquire()){
            System.out.println("Starting the server");
            try {
                //Process process = Runtime.getRuntime().exec("java -jar /Users/bharathgp/CouchbaseRestClient/target/CouchbaseRestClient-0.0.1-SNAPSHOT.jar");
                Process process = Runtime.getRuntime().exec("java -jar " + this.SDKServerLocation);
                Thread.sleep(5000);
                Process ps = Runtime.getRuntime().exec("ps aux | grep CouchbaseRestClient | grep -v grep");
                BufferedReader input = new BufferedReader(new InputStreamReader(ps.getInputStream()));
                String line;
                boolean started = false;
                while((line = input.readLine()) != null){
                    if(line.contains("CouchbaseRestClient")){
                        System.out.println("Started the server");
                        started = true;
                    }
                }
                if(!started){
                    System.out.println("Couldn't start the server");
                }
            } catch (InterruptedException | IOException e){
                e.printStackTrace();
            }
            finally {
                mutex.release();
            }
        }
        else {
            System.out.println("Another thread is starting the server");
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
