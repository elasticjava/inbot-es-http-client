package io.inbot.elasticsearch.crud;

import static com.github.jsonj.tools.JsonBuilder.array;
import static com.github.jsonj.tools.JsonBuilder.field;
import static com.github.jsonj.tools.JsonBuilder.object;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.StrictAssertions.assertThat;

import com.github.jsonj.JsonArray;
import com.github.jsonj.JsonObject;
import io.inbot.elasticsearch.bulkindexing.BulkIndexingOperations;
import io.inbot.elasticsearch.client.ElasticSearchIndex;
import io.inbot.elasticsearch.client.ElasticSearchType;
import io.inbot.elasticsearch.client.EsAPIClient;
import io.inbot.elasticsearch.exceptions.EsVersionConflictException;
import io.inbot.elasticsearch.testutil.DiyTestContext;
import io.inbot.elasticsearch.testutil.EsTestLauncher;
import io.inbot.elasticsearch.testutil.RandomHelper;
import io.inbot.utils.HashUtils;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test
public class CrudOperationsIntegrationTest {

    private EsAPIClient client;
    private CrudOpererationsFactory crudOperationsFactory;

    @BeforeMethod
    public void before() {
        EsTestLauncher.ensureEsIsUp();

        crudOperationsFactory = DiyTestContext.instance.crudOperationsFactory;
        client=DiyTestContext.instance.client;

    }

    public void shouldCreateUpdateGetAndDeleteEvent() {
        ElasticSearchIndex index = ElasticSearchIndex.create(RandomHelper.randomIndexName(), 1, "mapping-v1.json");
        ElasticSearchType type = ElasticSearchType.create(index, "test");

        CrudOperations dao = crudOperationsFactory.builder(type).enableRedisCache(false,10, "test").enableInMemoryCache(10000, 10).dao();
        JsonObject created = dao.create(randomObject(), false);
        String id = created.getString("id");
        dao.update(id, true, o -> {
            o.put("whatever", 42);
            return o;
        });

        JsonObject retrieved = dao.get(id);
        assertThat(retrieved).isNotNull();
        assertThat(retrieved.getLong("whatever")).isEqualTo(42l);
        dao.delete(id);
        assertThat(dao.get(id)).isNull();
    }

    @Test(expectedExceptions = EsVersionConflictException.class)
    public void shouldNotAllowDoubleCreation() {
        ElasticSearchIndex index = ElasticSearchIndex.create(RandomHelper.randomIndexName(), 1, "mapping-v1.json");
        ElasticSearchType type = ElasticSearchType.create(index, "test");

        CrudOperations dao = crudOperationsFactory.builder(type).dao();

        JsonObject event = randomObject();
        dao.create(event, false);
        dao.create(event, false);
    }

    private JsonObject randomObject() {
        return object(
                field("id", RandomHelper.randomId())
        );
    }

    public void shouldUseInMemoryCacheAndInvalidate() {
        ElasticSearchIndex index = ElasticSearchIndex.create(RandomHelper.randomIndexName(), 1, "mapping-v1.json");
        ElasticSearchType type = ElasticSearchType.create(index, "test");

        CrudOperations dao = crudOperationsFactory.builder(type).enableInMemoryCache(10000, 10).dao();

        GuavaCachingCrudDao cachingCrud = new GuavaCachingCrudDao(dao, 20, 20);
        String id = HashUtils.createId();
        JsonObject created = cachingCrud.create(object(field("id", id)), false);
        String createdVersion = created.getString("_version");
        assertThat(createdVersion).isEqualTo("1");
        assertThat(cachingCrud.get(id)).isEqualTo(created);
        JsonObject updated = cachingCrud.update(id, true, o -> {
           o.put("value", 42);
           return o;
        });
        assertThat(updated.getInt("value")).isEqualTo(42);
        assertThat(updated.getString("_version")).isEqualTo("2");
        assertThat(cachingCrud.get(id)).isEqualTo(updated);
        cachingCrud.delete(id);
        assertThat(cachingCrud.get(id)).isEqualTo(null);
    }

    public void shouldRecoverFromVersionConflict() throws InterruptedException {
        ElasticSearchIndex index = ElasticSearchIndex.create(RandomHelper.randomIndexName(), 1, "mapping-v1.json");
        ElasticSearchType type = ElasticSearchType.create(index, "test");

        CrudOperations dao = crudOperationsFactory.builder(type).enableRedisCache(false,10, "test").enableInMemoryCache(10000, 10).dao();

        String id = HashUtils.createId();

        final JsonObject o=object(
                field("id", id),
                field("foo",array())
                );
        dao.create(o, false);


        AtomicLong counter=new AtomicLong();
        ExecutorService threadPool = Executors.newFixedThreadPool(10);
        for(int i=0;i<3;i++) {
            threadPool.execute(() -> {
                try(BulkIndexingOperations bulkIndexer = dao.bulkIndexer()) {
                    for(int j=0;j<3;j++) {
                        long val = counter.incrementAndGet();
                        bulkIndexer.update(id, ""+666,null , o, obj -> {
                            JsonArray arr = obj.getOrCreateSet("foo");
                            arr.add(val);
                            obj.put("foo", arr);
                            obj.put("recover_from_version_conflict", true);
                            return obj;
                        });
                    }
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });
        }
        client.refresh();
        threadPool.shutdown();
        threadPool.awaitTermination(20, TimeUnit.SECONDS);
        JsonObject updated = dao.get(false, id); // make sure to get the non cached version :-)
        assertThat(counter.get()).isEqualTo(9l);
        assertThat(updated.get("recover_from_version_conflict",false)).isTrue();
    }

    public void shouldUpdate() throws InterruptedException {
        ElasticSearchIndex index = ElasticSearchIndex.create(RandomHelper.randomIndexName(), 1, "mapping-v1.json");
        ElasticSearchType type = ElasticSearchType.create(index, "test");

        CrudOperations dao = crudOperationsFactory.builder(type).enableRedisCache(false,10, "test").enableInMemoryCache(10000, 10).dao();

        String id = HashUtils.createId();

        final JsonObject o=object(
                field("id", id),
                field("foo",array())
                );
        dao.create(o, false);


        AtomicLong counter=new AtomicLong();
        ExecutorService threadPool = Executors.newFixedThreadPool(10);
        for(int i=0;i<3;i++) {
            threadPool.execute(() -> {
                try(BulkIndexingOperations bulkIndexer = dao.bulkIndexer()) {
                    AtomicInteger version=new AtomicInteger(1);
                    for(int j=0;j<3;j++) {
                        long val = counter.incrementAndGet();
                        bulkIndexer.update(id, ""+version.get(),null , o, obj -> {
                            JsonArray arr = obj.getOrCreateSet("foo");
                            arr.add(val);
                            obj.put("foo", arr);
                            return obj;
                        });
                    }
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });
        }
        client.refresh();
        threadPool.shutdown();
        threadPool.awaitTermination(20, TimeUnit.SECONDS);
        JsonObject updated = dao.get(false, id); // make sure to get the non cached version :-)
        assertThat(counter.get()).isEqualTo(9l);
        assertThat(updated.getArray("foo")).isNotNull(); // should be there because version was correct
    }

    public void shouldDoMultiGet() {
        ElasticSearchIndex index = ElasticSearchIndex.create(RandomHelper.randomIndexName(), 1, "mapping-v1.json");
        ElasticSearchType type = ElasticSearchType.create(index, "test");

        CrudOperations dao = crudOperationsFactory.builder(type).enableRedisCache(false,10, "test").enableInMemoryCache(10000, 10).dao();

        JsonArray objects=array();
        for(int i=0; i<10; i++) {
            objects.add(dao.create(object(
                field("id", HashUtils.createId()),
                field("foo",array())
                ), false));
        }
        String[] ids = objects.streamObjects().map(o -> o.getString("id")).toArray(size -> new String[size]);

        JsonArray retrieved = dao.mget(true, ids);
        assertThat(retrieved.size()).isEqualTo(10);
    }

    public void shouldKeepTrackOfModifiedIds() {
        ElasticSearchIndex index = ElasticSearchIndex.create(RandomHelper.randomIndexName(), 1, "mapping-v1.json");
        ElasticSearchType type = ElasticSearchType.create(index, "test");

        CrudOperations dao = crudOperationsFactory.builder(type).enableRedisCache(false,10, "test").enableInMemoryCache(10000, 10).dao();

        JsonArray ids = array();
        for(int i=0; i<10; i++) {
            String id = HashUtils.createId();
            dao.create(object(
                    field("id", id),
                    field("message","foo")
                    ), true);
            ids.add(id);
        }
        client.refresh();
        for(int i=0; i<10; i+=2) {
            dao.update(ids.get(i).asString(), true, object -> {
                object.put("message", "bar");
                return object;
            });
        }
        // we could use this set to swap out stale search results with their current version that we can get from the dao
        Set<String> recentlyModifiedIds = dao.recentlyModifiedIds();
        // we just modified some objects so should be same size
        assertThat(recentlyModifiedIds.size()).isEqualTo(5);
    }
}
