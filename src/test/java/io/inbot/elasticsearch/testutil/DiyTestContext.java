package io.inbot.elasticsearch.testutil;

import com.github.jsonj.tools.JsonParser;
import io.inbot.elasticsearch.client.EsAPIClient;
import io.inbot.elasticsearch.client.HttpEsAPIClient;
import io.inbot.elasticsearch.crud.CrudOpererationsFactory;
import io.inbot.elasticsearch.jsonclient.JsonJRestClient;
import io.inbot.redis.FakeJedisPool;
import io.inbot.redis.RedisBackedCircularStack;

/**
 * Since we don't have spring in this project and since I don't like to copy paste a lot of bean initialation across my
 * tests, we implement DIY dependency injection.
 * 
 * It works very simple. We have a per jvm application context, it has beans, they get initialized in this class, you
 * use them where you need them by
 * 'looking them up' and injecting them where you need them.
 */
public class DiyTestContext {
    public static DiyTestContext instance = new DiyTestContext();

    public final JsonParser parser = new JsonParser();
    public final JsonJRestClient simpleClient = JsonJRestClient.simpleClient(EsTestLauncher.ES_URL);
    public final EsAPIClient client = new HttpEsAPIClient(simpleClient, parser, 10000);

    public final FakeJedisPool fakeJedisPool = new FakeJedisPool();
    public final RedisBackedCircularStack redisBackedCircularStack = new RedisBackedCircularStack(fakeJedisPool, 10, 60);

    public final CrudOpererationsFactory crudOperationsFactory = new CrudOpererationsFactory(client, parser, fakeJedisPool, redisBackedCircularStack);

    private DiyTestContext() {
    }

}
