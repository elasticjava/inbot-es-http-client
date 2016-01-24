package io.inbot.elasticsearch.crud;

import com.github.jsonj.tools.JsonParser;
import io.inbot.elasticsearch.client.ElasticSearchType;
import io.inbot.elasticsearch.client.EsAPIClient;
import io.inbot.elasticsearch.crud.CrudOperations;
import io.inbot.elasticsearch.crud.EsCrudDao;
import io.inbot.elasticsearch.crud.EsParentChildCrudDao;
import io.inbot.elasticsearch.crud.GuavaCachingChildCrudDao;
import io.inbot.elasticsearch.crud.GuavaCachingCrudDao;
import io.inbot.elasticsearch.crud.ParentChildCrudOperations;
import io.inbot.elasticsearch.crud.RedisCachingCrudDao;
import io.inbot.elasticsearch.crud.RedisCachingParentChildCrudDao;
import io.inbot.redis.RedisBackedCircularStack;
import io.inbot.redis.RedisCache;
import redis.clients.jedis.JedisPool;

public class CrudOperationsBuilder {
    private final ElasticSearchType indexType;
    private final EsAPIClient esApiClient;

    private final JsonParser parser;
    private final JedisPool jedisPool;
    private final RedisBackedCircularStack redisBackedCircularStack;
    private int retryUpdates = 5;
    private boolean redis = false;
    private int redisExpireAfterWriteInSeconds = 10;
    private String redisPrefix = "none";
    private boolean inMemoryCache = false;
    private int maxInMemoryItems;
    private int inMemoryExpireAfterWriteSeconds;

    /**
     * Don't call this directly and use the CrudOperationsFactory instead.
     */
    CrudOperationsBuilder(EsAPIClient esApiClient, JsonParser parser, JedisPool jedisPool, RedisBackedCircularStack redisBackedCircularStack,
            ElasticSearchType indexType) {
        this.esApiClient = esApiClient;
        this.parser = parser;
        this.jedisPool = jedisPool;
        this.redisBackedCircularStack = redisBackedCircularStack;
        this.indexType = indexType;
    }

    public CrudOperationsBuilder retryUpdates(int times) {
        retryUpdates = times;
        return this;
    }

    public CrudOperationsBuilder enableRedisCache(boolean enableRedis, int expireAfterWriteInSeconds, String prefix) {
        if(enableRedis) {
            this.redisExpireAfterWriteInSeconds = expireAfterWriteInSeconds;
            this.redisPrefix = prefix;
            redis = true;
        }
        return this;
    }

    public CrudOperationsBuilder enableInMemoryCache(int maxItems, int expireAfterWriteSeconds) {
        this.maxInMemoryItems = maxItems;
        this.inMemoryExpireAfterWriteSeconds = expireAfterWriteSeconds;
        inMemoryCache = true;
        return this;
    }

    public CrudOperations dao() {
        if(indexType.parentChild()) {
            throw new IllegalStateException("type " + indexType + " specifies parent child relations");
        }
        CrudOperations dao = new EsCrudDao(indexType, esApiClient, redisBackedCircularStack, retryUpdates);
        if(redis) {
            dao = new RedisCachingCrudDao(dao, new RedisCache(jedisPool, parser, redisPrefix, indexType.version(), redisExpireAfterWriteInSeconds));
        }
        if(inMemoryCache) {
            dao = new GuavaCachingCrudDao(dao, maxInMemoryItems, inMemoryExpireAfterWriteSeconds);
        }

        return dao;
    }

    public ParentChildCrudOperations childDao() {
        if(!indexType.parentChild()) {
            throw new IllegalStateException("type " + indexType + " does not specify parent child relations");
        }

        ParentChildCrudOperations dao = new EsParentChildCrudDao(indexType, esApiClient, redisBackedCircularStack, retryUpdates);
        if(redis) {
            dao = new RedisCachingParentChildCrudDao(dao, new RedisCache(jedisPool, parser, redisPrefix, indexType.version(), redisExpireAfterWriteInSeconds));
        }
        if(inMemoryCache) {
            dao = new GuavaCachingChildCrudDao(dao, maxInMemoryItems, inMemoryExpireAfterWriteSeconds);
        }

        return dao;
    }
}