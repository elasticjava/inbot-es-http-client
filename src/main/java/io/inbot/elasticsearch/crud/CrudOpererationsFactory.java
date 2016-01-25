package io.inbot.elasticsearch.crud;

import com.github.jsonj.tools.JsonParser;
import io.inbot.elasticsearch.client.ElasticSearchType;
import io.inbot.elasticsearch.client.EsAPIClient;
import io.inbot.redis.RedisBackedCircularStack;
import io.inbot.redis.RedisCache;
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

    public static class CrudOperationsBuilder {
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

        private CrudOperationsBuilder(EsAPIClient esApiClient, JsonParser parser, JedisPool jedisPool, RedisBackedCircularStack redisBackedCircularStack,
                ElasticSearchType indexType) {
            this.esApiClient = esApiClient;
            this.parser = parser;
            this.jedisPool = jedisPool;
            this.redisBackedCircularStack = redisBackedCircularStack;
            this.indexType = indexType;
        }

        /**
         * @param times Amount of times updates should be retried in case of a version conflict
         * @return builder
         */
        public CrudOperationsBuilder retryUpdates(int times) {
            retryUpdates = times;
            return this;
        }

        /**
         * Add a redis cache.
         * @param enableRedis true if you want caching in redis
         * @param expireAfterWriteInSeconds ttl of the objects in redis
         * @param prefix key prefix that is to be used
         * @return builder
         */
        public CrudOperationsBuilder enableRedisCache(boolean enableRedis, int expireAfterWriteInSeconds, String prefix) {
            if(enableRedis) {
                this.redisExpireAfterWriteInSeconds = expireAfterWriteInSeconds;
                this.redisPrefix = prefix;
                redis = true;
            }
            return this;
        }

        /**
         * Add guava cache.
         * @param maxItems maximum number of items to keep in memory.
         * @param expireAfterWriteSeconds ttl of in memory objects
         * @return builder
         */
        public CrudOperationsBuilder enableInMemoryCache(int maxItems, int expireAfterWriteSeconds) {
            this.maxInMemoryItems = maxItems;
            this.inMemoryExpireAfterWriteSeconds = expireAfterWriteSeconds;
            inMemoryCache = true;
            return this;
        }

        /**
         * @return CrudOperations for objects without a parent.
         */
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

        /**
         * @return ParentChildCrudOperations for objects with a parent.
         */
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
}