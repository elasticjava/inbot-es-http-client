package io.inbot.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class FakeJedisPool extends JedisPool {
    private final FakeJedis fakeJedis = new FakeJedis();
    public FakeJedisPool() {
        super("localhost");

    }

    @Override
    public Jedis getResource() {
        return fakeJedis;
    }
}
