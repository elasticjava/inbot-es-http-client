package io.inbot.redis;

import static com.github.jsonj.tools.JsonBuilder.array;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.github.jsonj.JsonArray;
import com.github.jsonj.JsonObject;
import com.github.jsonj.tools.JsonParser;
import io.inbot.utils.CompressionUtils;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.commons.lang3.Validate;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

/**
 * Simple cache that uses Redis.
 */
public class RedisCache implements MetricSet {
    private final JedisPool jedisPool;
    private final String keyPrefix;
    private final int expirationInSeconds;
    private final JsonParser parser;
    private final Meter notFoundMeter;
    private final Timer mgetTimer;
    private final Timer getTimer;
    private final Timer putTimer;
    private final Timer delTimer;
    private final int version;

    /**
     * Object cache that uses redis.
     *
     * Important: bulkindexing bypasses the cache and all keys will be stale after bulkindexing. Using clearAll may be expensive.
     *
     * @param jedisPool
     * @param parser
     * @param keyPrefix
     *            use something short, stick with the convention i/g/v/yourkey so the prefix is i/g/v where v is the
     *            current version of the index. This way all the keys expire when we upgrade an index.
     * @param expirationInSeconds keep this low to ensure any cache coherence issues go away in a reasonable time.
     */
    public RedisCache(JedisPool jedisPool, JsonParser parser, String keyPrefix, int version, int expirationInSeconds) {
        this.jedisPool = jedisPool;
        this.parser = parser;
        this.keyPrefix = keyPrefix;
        this.version = version;
        this.expirationInSeconds = expirationInSeconds;
        missMeter = new Meter();
        hitMeter = new Meter();
        notFoundMeter = new Meter();
        mgetTimer = new Timer();
        getTimer = new Timer();
        putTimer = new Timer();
        delTimer = new Timer();
    }

    @Override
    public Map<String, Metric> getMetrics() {
        Map<String, Metric> metrics = new HashMap<>();
        String prefix = "redis."+keyPrefix.replace('/', '.');
        metrics.put(prefix+".get", getTimer);
        metrics.put(prefix+".mget", mgetTimer);
        metrics.put(prefix+".put", putTimer);
        metrics.put(prefix+".del", delTimer);
        metrics.put(prefix+".miss", missMeter);
        metrics.put(prefix+".hit", hitMeter);
        metrics.put(prefix+".notfound", notFoundMeter);
        Gauge<Double> hitRatioGauge = new Gauge<Double>() {

            @Override
            public Double getValue() {
                double getCount = getTimer.getCount();
                double hitCount = hitMeter.getCount();
                return hitCount/getCount;
            }
        };
        metrics.put(prefix+".hitratio", hitRatioGauge);

        return metrics;
    }

    private byte[] key(String key) {
        return (keyPrefix + "/" + version + "/" + key).getBytes(utf8);
    }

    Charset utf8 = Charset.forName("UTF-8");
    private final Meter missMeter;
    private final Meter hitMeter;

    public void put(JsonObject value) {
        try(Context context = putTimer.time()) {
            String id = value.getString("id");
            Validate.notEmpty(id);
            try(Jedis resource = jedisPool.getResource()) {
                resource.setex(key(id), expirationInSeconds, CompressionUtils.compress(value.toString().getBytes(utf8)));
            } catch (JedisException e) {
                // make sure we can find back jedis related stuff in kibana
                throw new IllegalStateException("problem connecting to jedis", e);
            }
        }
    }

    public Optional<JsonObject> get(String key) {
        try(Context context = getTimer.time()) {
            try(Jedis resource = jedisPool.getResource()) {
                byte[] value = resource.get(key(key));
                if(value != null) {
                    try {
                        hitMeter.mark();
                        return Optional.of(parser.parseObject(new String(CompressionUtils.decompress(value), utf8)));
                    } catch (IllegalArgumentException e) {
                        throw new IllegalStateException("invalid json returned from redis");
                    }
                }
            } catch (JedisException e) {
                // make sure we can find back jedis related stuff in kibana
                throw new IllegalStateException("problem connecting to jedis", e);
            }
            notFoundMeter.mark();
            return Optional.empty();
        }
    }

    public Optional<JsonObject> get(String key, Function<String, JsonObject> producer) {
        Optional<JsonObject> optional = get(key);
        if(optional.isPresent()) {
            return optional;
        } else {
            JsonObject value = producer.apply(key);
            if(value == null) {
                notFoundMeter.mark();
                return Optional.empty();
            } else {
                put(value);
                return Optional.of(value);
            }
        }
    }

    public JsonArray mget(String...keys) {
        try(Context context = mgetTimer.time()) {
            JsonArray results=array();
            try(Jedis resource = jedisPool.getResource()) {
                byte[][] byteKeys = Arrays.stream(keys).map(key -> key(key)).toArray(size -> new byte[size][]);
                List<byte[]> redisResults = resource.mget(byteKeys);
                if(redisResults!=null) {
                    for(byte[] blob:redisResults) {
                        if(blob != null) {
                            // some results will be null
                            results.add(parser.parseObject(new String(CompressionUtils.decompress(blob), utf8)));
                        }
                    }
                }
            } catch (JedisException e) {
                // make sure we can find back jedis related stuff in kibana
                throw new IllegalStateException("problem connecting to jedis", e);
            }
            notFoundMeter.mark();
            return results;
        }
    }

    public void delete(String key) {
        try(Context context = delTimer.time()) {
            try(Jedis resource = jedisPool.getResource()) {
                resource.del(key(key));
            } catch (JedisException e) {
                // make sure we can find back jedis related stuff in kibana
                throw new IllegalStateException("problem connecting to jedis", e);
            }
        }
    }

    /**
     * Note, this can be slooooow if you have many keys.
     */
    public void clearAll() {
        try(Jedis resource = jedisPool.getResource()) {
            resource.eval("for i, name in ipairs(redis.call('KEYS', '"+keyPrefix+"/*')) do redis.call('DEL', name); end");
        } catch (JedisException e) {
            // make sure we can find back jedis related stuff in kibana
            throw new IllegalStateException("problem connecting to jedis", e);
        }
    }
}


