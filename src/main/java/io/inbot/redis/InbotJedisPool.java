package io.inbot.redis;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer;
import io.inbot.utils.ArrayFoo;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class InbotJedisPool extends JedisPool implements MetricSet {
    private static final Logger LOG = LoggerFactory.getLogger(InbotJedisPool.class);
    private final Timer jedisSessionTimer=new Timer();
    private final Counter poolFailCounter=new Counter();
    private final Counter poolRetryCounter=new Counter();

    private final Set<String> jedisMethods=ArrayFoo.setOf("rpush", "lrange", "zadd","zremrangeByScore", "zscore", "get", "del","lpush","brpoplpush","lrem","llen","setex","set");
    private final Map<String, Timer> timerMap;
    private final String redisHost;
    private final int redisPort;
    private final String redisPassword;
    private final int redisDatabase;

    private class MeasuringJedisHandler implements MethodInterceptor {
        private final Jedis jedis;
        long start=System.nanoTime();

        public MeasuringJedisHandler(Jedis jedis) {
            this.jedis = jedis;
        }

        @Override
        public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
            String methodName = method.getName();
            if("close".equals(methodName)) {
                jedisSessionTimer.update(System.nanoTime()-start, TimeUnit.NANOSECONDS);
                return method.invoke(jedis, args);
            } else if(jedisMethods.contains(methodName)) {
                long methodStart=System.nanoTime();
                Object returnValue = method.invoke(jedis, args);
                timerMap.get(methodName).update(System.nanoTime()-methodStart, TimeUnit.NANOSECONDS);;
                return returnValue;

            } else {
                return method.invoke(jedis, args);
            }
        }
    }

    public InbotJedisPool(GenericObjectPoolConfig config, String host, int port, int connectionTimeout, int soTimeout, String password, int database, String clientName) {
        super(config, host, port, connectionTimeout, soTimeout, password, database, clientName);
        this.redisHost = host;
        this.redisPort = port;
        this.redisPassword = password;
        this.redisDatabase = database;
        timerMap = new ConcurrentHashMap<>();
        jedisMethods.forEach(m -> timerMap.put(m, new Timer()));
    }

    @Override
    public Jedis getResource() {
        // add a simplistic retry strategy so we don't fail on the occasional pool timeout
        Jedis resource = null;
        try {
            resource= super.getResource();
        } catch (RuntimeException e) {
            try {
                Thread.sleep(500); // give it half a second to recover
            } catch (InterruptedException ex) {
                throw new IllegalStateException(ex);
            }
            try {
                resource= super.getResource();
                poolRetryCounter.inc();
            } catch (RuntimeException e2) {
                LOG.error("redis connect failure after retry once. Host: '" + redisHost + "' port: '" + redisPort + "' redis db: '" + redisDatabase + "' redis password: '" + redisPassword +"'");
                poolFailCounter.inc();
                // rethrow and let things escalate
                throw e2;
            }
        }

        return (Jedis) Enhancer.create(Jedis.class, new MeasuringJedisHandler(resource));
    }

    @Override
    public Map<String, Metric> getMetrics() {
        HashMap<String, Metric> metrics = new HashMap<>();
        metrics.put("session_timer", jedisSessionTimer);
        metrics.put("pool_retry_success", poolRetryCounter);
        metrics.put("pool_retry_fail", poolFailCounter);
        metrics.putAll(timerMap);

        return metrics;
    };
}
