package io.inbot.elasticsearch.testutil;

import com.github.jsonj.tools.JsonParser;
import io.inbot.elasticsearch.client.EsAPIClient;
import io.inbot.elasticsearch.client.HttpEsAPIClient;
import io.inbot.elasticsearch.crud.CrudOpererationsFactory;
import io.inbot.elasticsearch.jsonclient.JsonJRestClient;
import io.inbot.redis.FakeJedisPool;
import io.inbot.redis.RedisBackedCircularStack;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

/**
 * Since we don't have spring in this project and since I don't like to copy paste a lot of bean initialation across my
 * tests, we implement DIY dependency injection.
 * 
 * It works very simple. We have a per jvm application context, it has beans, they get initialized in this class, you
 * use them where you need them by 'looking them up' and injecting them where you need them.
 */
public class DiyTestContext {
    private static final Logger LOG = LoggerFactory.getLogger(DiyTestContext.class);

    public static DiyTestContext instance = new DiyTestContext();

    public final JsonParser parser = new JsonParser();
    public final JsonJRestClient simpleClient = JsonJRestClient.simpleClient(EsTestLauncher.ES_URL);
    public final EsAPIClient client = new HttpEsAPIClient(simpleClient, parser, 10000);

    public final JedisPool jedisPool;
    public final RedisBackedCircularStack redisBackedCircularStack;
    public final CrudOpererationsFactory crudOperationsFactory;

    private DiyTestContext() {
        String redisEnabledProperty = System.getProperty("redis.enabled");
        if(StringUtils.isNotBlank(redisEnabledProperty) && Boolean.valueOf(redisEnabledProperty)) {
            LOG.info("using real jedis");
            jedisPool = new JedisPool();
        } else {
            LOG.info("using fake jedis");
            jedisPool = new FakeJedisPool();
        }
        redisBackedCircularStack = new RedisBackedCircularStack(jedisPool, 10, 60);
        crudOperationsFactory = new CrudOpererationsFactory(client, parser, jedisPool, redisBackedCircularStack);
    }
}
