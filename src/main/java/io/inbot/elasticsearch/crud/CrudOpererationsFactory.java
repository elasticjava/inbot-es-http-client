package io.inbot.elasticsearch.crud;

import com.github.jsonj.tools.JsonParser;
import io.inbot.elasticsearch.client.ElasticSearchType;
import io.inbot.elasticsearch.client.EsAPIClient;
import io.inbot.redis.RedisBackedCircularStack;
import redis.clients.jedis.JedisPool;

public class CrudOpererationsFactory {
    private final EsAPIClient esApiClient;
    private final JsonParser parser;
    private final JedisPool jedisPool;
    private final RedisBackedCircularStack redisBackedCircularStack;

    public CrudOpererationsFactory(EsAPIClient esApiClient, JsonParser parser, JedisPool jedisPool, RedisBackedCircularStack redisBackedCircularStack) {
        this.esApiClient = esApiClient;
        this.parser = parser;
        this.jedisPool = jedisPool;
        this.redisBackedCircularStack = redisBackedCircularStack;
    }

    public CrudOperationsBuilder builder(ElasticSearchType type) {
        return new CrudOperationsBuilder(esApiClient, parser, jedisPool, redisBackedCircularStack, type);
    }
}