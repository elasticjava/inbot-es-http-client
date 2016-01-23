package io.inbot.redis;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisException;

/**
 * Implement circular stack with fallback to in memory one for tests.
 */
public class RedisBackedCircularStack {
    private final JedisPool jedisPool;
    private final int capacity;
    private final boolean fake;
    private Map<String, ConcurrentLinkedQueue<String>> circularStackMap;
    private final int expireInSeconds;

    public RedisBackedCircularStack(JedisPool jedisPool, int capacity, int expireInSeconds) {
        this.jedisPool = jedisPool;
        this.capacity = capacity;
        this.expireInSeconds = expireInSeconds;
        fake = jedisPool instanceof FakeJedisPool;
        if(fake) {
            circularStackMap = new ConcurrentHashMap<>();
        } else {
            circularStackMap = null;
        }
    }

    public void add(String key, String value) {
        if(fake) {
            ConcurrentLinkedQueue<String> stack = circularStackMap.get(key);
            if(stack==null) {
                synchronized(circularStackMap) {
                    stack = circularStackMap.get(key);
                    if(stack==null) {
                        stack = new ConcurrentLinkedQueue<String>();
                        circularStackMap.put(key, stack);
                    }
                }
            }
            stack.add(value);
            while(stack.size() > capacity) {
                stack.poll();
            }
        } else {
            try(Jedis jedis = jedisPool.getResource()) {
                Transaction transaction = jedis.multi();
                transaction.lpush(key, value);
                transaction.ltrim(key, 0, capacity); // right away trim to capacity so that we can pretend it is a circular list
                transaction.expire(key, expireInSeconds); // don't keep the data forever
                transaction.exec();
            } catch (JedisException e) {
                // make sure we can find back jedis related stuff in kibana
                throw new IllegalStateException("problem connecting to jedis", e);
            }
        }
    }

    /**
     * @param key key of the list
     * @return list of ids in reverse order of addition (latest first).
     */
    public List<String> list(String key) {
        if(fake) {
            ConcurrentLinkedQueue<String> stack = circularStackMap.get(key);
            if(stack==null) {
                List<String> result = Collections.unmodifiableList(Lists.reverse(Lists.newArrayList()));
                return result;
            }
            else {
                // we have to reverse because ConcurrentLinkedQueue appends at end of list and has no equivalent of lpush
                List<String> result = Collections.unmodifiableList(Lists.newArrayList(stack));
                return result;
            }
        }
        ArrayList<String> modified = new ArrayList<>();
        try(Jedis jedis = jedisPool.getResource()) {
            List<String> lrange = jedis.lrange(key, 0, capacity);
            if(lrange != null) {
                for(String e: lrange) {
                    modified.add(e);
                }
            }
        } catch (JedisException e) {
            // make sure we can find back jedis related stuff in kibana
            throw new IllegalStateException("problem connecting to jedis", e);
        }
        return Collections.unmodifiableList(modified);
    }
}
