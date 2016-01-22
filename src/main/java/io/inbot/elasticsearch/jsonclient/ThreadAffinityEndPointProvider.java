package io.inbot.elasticsearch.jsonclient;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.http.client.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple EndpointProvider implementation that tries to use the same endpoint for each thread that calls it and fails
 * over sensibly if that endpoint becomes unavailable.
 */
public class ThreadAffinityEndPointProvider implements EndpointProvider {
    private static final Logger LOG = LoggerFactory.getLogger(ThreadAffinityEndPointProvider.class);

    private final String[] endpoints;
    private final Function<String, Boolean> validateFunction;

    private final Map<String,Long> endpointStatus = Maps.newConcurrentMap();

    /**
     * @param validateFunction function that returns true if the endpoint is valid and false if it is unavailable. This
     * function is called every time the endpoint is used. You may want to ensure that you only do expensive network lookups only once in a while inside the
     * function. Use e.g. our {@link CachingEndpointPinger} to implement a sensible validation strategy or use periodicallyPingingProvider(..) to get
     * it preconfigured.
     * @param endPoints list of endPoints
     */
    public ThreadAffinityEndPointProvider(Function<String,Boolean> validateFunction, String...endPoints) {
        this.validateFunction = validateFunction;
        this.endpoints = endPoints;
    }

    /**
     * @param httpClient an HttpClient instance
     * @param pingDelayInSeconds urls will not be pinged again until this period expires.
     * @param endPoints end points to provide
     * @return a thread affinity provider that periodically pings endpoints to see if they are still alive as a means of validation.
     */
    public static ThreadAffinityEndPointProvider periodicallyPingingProvider(HttpClient httpClient,int pingDelayInSeconds, String...endPoints) {
        LoadingCache<String,Boolean> pinger = CacheBuilder.newBuilder().expireAfterAccess(pingDelayInSeconds, TimeUnit.SECONDS).build(new CachingEndpointPinger(httpClient));
        return new ThreadAffinityEndPointProvider(url -> {
            try {
                return pinger.get(url);
            } catch (ExecutionException e) {
                return false;
            }
        }, endPoints);
    }

    private boolean validate(String endPoint) {
        boolean status = validateFunction.apply(endPoint);
        if(status) {
            // recover if it previously failed
            endpointStatus.remove(endPoint);
        }
        return status;
    }

    @Override
    public void failEndpoint(String url) {
        LOG.error("unhealthy elasticsearch endpoint " + url);
        endpointStatus.put(url, System.currentTimeMillis());
    }

    @Override
    public String endPoint() {
        if(endpoints.length ==1) {
            return endpoints[0];
        } else {
            // FIXME failover?
            int hashCode = Thread.currentThread().hashCode();
            // always try to use the same url for the same thread; that way the same node handles all queries for one request
            String preferredUrl = endpoints[Math.abs(hashCode % endpoints.length) ];

            if(validate(preferredUrl)) {
                return preferredUrl;
            } else {
                // return first healthy url
                for(String url: endpoints) {
                    if(validate(url)) {
                        return url;
                    }
                }
            }
            // if none of the urls are healthy, return the preferredUrl anyway
            return preferredUrl;
        }
    }
}
