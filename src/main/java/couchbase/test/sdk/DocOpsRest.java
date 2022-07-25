package couchbase.test.sdk;

import reactor.util.function.Tuple2;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class DocOpsRest {

    static class CURDOptions {
        public Integer timeout;
        public String time_unit;
        public String retryStrategy;
        public String durabilityLevel;
        public int persist_to;
        public int replicate_to;
        public Integer expiry;
        public String expiry_unit;
        public CURDOptions(Integer timeout, String time_unit, String retryStrategy, String durabilityLevel){
            this.timeout = timeout;
            this.time_unit = time_unit;
            this.retryStrategy = retryStrategy;
            this.durabilityLevel = durabilityLevel;
        }
        public CURDOptions(Integer timeout, String time_unit, String retryStrategy, String durabilityLevel, Integer expiry, String expiry_unit){
            this.timeout = timeout;
            this.time_unit = time_unit;
            this.retryStrategy = retryStrategy;
            this.durabilityLevel = durabilityLevel;
            this.expiry = expiry;
            this.expiry_unit = expiry_unit;
        }

        public CURDOptions(Integer timeout, String time_unit, String retryStrategy){
            this.timeout = timeout;
            this.time_unit = time_unit;
            this.retryStrategy = retryStrategy;
        }
    }

    public static class WriteOpBody {
        public Server server;
        public List<Tuple2<String, Object>> documents;
        public CURDOptions curdOptions;
        public WriteOpBody(Server server, List<Tuple2<String, Object>> documents, CURDOptions curdOptions){
            this.server = server;
            this.documents = documents;
            this.curdOptions = curdOptions;
        }
    }

    public static class ReadOpBody {
        public Server server;
        public List<String> keys;
        public CURDOptions curdOptions;
        public ReadOpBody(Server server, List<String> keys, CURDOptions curdOptions){
            this.server = server;
            this.keys = keys;
            this.curdOptions = curdOptions;
        }
    }

    public List<Result> bulkInsert(RestSDKClient sdkClient, String bucket, String scope,
                                   String collection, List<Tuple2<String, Object>> documents,
                                   Integer timeout, String time_unit, String retryStrategy, String durabilityLevel){
        CURDOptions curdOptions = new CURDOptions(timeout, time_unit, retryStrategy, durabilityLevel);
        WriteOpBody postBody = new WriteOpBody(sdkClient.master, documents, curdOptions);
        String api = "bulkInsert/" + bucket + "/" + scope + "/" + collection;
        List<Result> results;
        results = (List<Result>) sdkClient.postRequest(api, postBody, List.class);
        return results;
    }

    public List<Result> bulkUpsert(RestSDKClient sdkClient, String bucket, String scope,
                                   String collection, List<Tuple2<String, Object>> documents,
                                   Integer timeout, String time_unit, String retryStrategy, String durabilityLevel){
        CURDOptions curdOptions = new CURDOptions(timeout, time_unit, retryStrategy, durabilityLevel);
        WriteOpBody postBody = new WriteOpBody(sdkClient.master, documents, curdOptions);
        String api = "bulkUpsert/" + bucket + "/" + scope + "/" + collection;
        List<Result> results;
        results = (List<Result>) sdkClient.postRequest(api, postBody, List.class);
        return results;
    }

    public List<Tuple2<String, Object>> bulkGets(RestSDKClient sdkClient, String bucket, String scope,
                                                 String collection, List<Tuple2<String, Object>> documents,
                                                 Integer timeout, String time_unit, String retryStrategy){
        CURDOptions curdOptions = new CURDOptions(timeout, time_unit, retryStrategy);
        WriteOpBody postBody = new WriteOpBody( sdkClient.master, documents, curdOptions);
        String api = "bulkUpsert/" + bucket + "/" + scope + "/" + collection;
        List<Tuple2<String, Object>> results;
        results = (List<Tuple2<String, Object>>) sdkClient.postRequest(api, postBody, List.class);
        return results;
    }

    public List<Result> bulkDelete(RestSDKClient sdkClient, String bucket, String scope,
                                   String collection, List<String > keys, Integer timeout, String time_unit,
                                   String retryStrategy, String durabilityLevel){
        CURDOptions curdOptions = new CURDOptions(timeout, time_unit, retryStrategy, durabilityLevel);
        ReadOpBody postBody = new ReadOpBody( sdkClient.master, keys, curdOptions);
        String api = "bulkDelete/" + bucket + "/" + scope + "/" + collection;
        List <Result> results;
        results = (List<Result>) sdkClient.postRequest(api, postBody, List.class);
        return results;
    }

    public List<ConcurrentHashMap<String, Object>> bulkReplace(RestSDKClient sdkClient, String bucket, String scope,
                                                               String collection, List<Tuple2<String, Object>> documents,
                                                               Integer timeout, String time_unit, String retryStrategy, String durabilityLevel) {
        CURDOptions curdOptions = new CURDOptions(timeout, time_unit, retryStrategy, durabilityLevel);
        WriteOpBody postBody = new WriteOpBody( sdkClient.master, documents, curdOptions);
        String api = "bulkReplace/" + bucket + "/" + scope + "/" + collection;
        List<ConcurrentHashMap<String, Object>> results;
        results = (List<ConcurrentHashMap<String, Object>>) sdkClient.postRequest(api, postBody, List.class);
        return results;
    }

    public List<ConcurrentHashMap<String, Object>> bulkTouch(RestSDKClient sdkClient, String bucket, String scope,
                                                               String collection, List<String > keys,
                                                               Integer timeout, String time_unit, String retryStrategy, String durabilityLevel) {
        CURDOptions curdOptions = new CURDOptions(timeout, time_unit, retryStrategy, durabilityLevel);
        ReadOpBody postBody = new ReadOpBody( sdkClient.master, keys, curdOptions);
        String api = "bulkTouch/" + bucket + "/" + scope + "/" + collection;
        List<ConcurrentHashMap<String, Object>> results;
        results = (List<ConcurrentHashMap<String, Object>>) sdkClient.postRequest(api, postBody, List.class);
        return results;
    }


}
