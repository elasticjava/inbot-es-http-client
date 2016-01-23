package io.inbot.elasticsearch.bulkindexing;

import static com.github.jsonj.tools.JsonBuilder.field;
import static com.github.jsonj.tools.JsonBuilder.object;
import static org.assertj.core.api.StrictAssertions.assertThat;

import com.github.jsonj.JsonObject;
import com.github.jsonj.tools.JsonParser;
import io.inbot.elasticsearch.client.EsAPIClient;
import io.inbot.elasticsearch.client.HttpEsAPIClient;
import io.inbot.elasticsearch.client.QueryBuilder;
import io.inbot.elasticsearch.client.SearchResponse;
import io.inbot.elasticsearch.jsonclient.JsonJRestClient;
import io.inbot.elasticsearch.testutil.EsTestLauncher;
import io.inbot.elasticsearch.testutil.RandomHelper;
import io.inbot.elasticsearch.testutil.RandomIndexHelper;
import io.inbot.utils.HashUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test
public class BulkIndexerIntegrationTest {

    private EsAPIClient client;
    private JsonParser jsonparser;

    @BeforeMethod
    public void before() throws IOException {
        EsTestLauncher.ensureEsIsUp();
        JsonJRestClient simpleClient = JsonJRestClient.simpleClient(EsTestLauncher.ES_URL);
        jsonparser = new JsonParser();
        client = new HttpEsAPIClient(simpleClient, jsonparser, 10000);

    }

    public void shouldIndexMultipleJsonObjects() throws IOException {
        RandomIndexHelper index = RandomIndexHelper.index();

        try (BulkIndexingOperations bi = client.bulkIndexer(index.index, index.type, 5, 4)) {
            for (int i = 0; i < 26; i++) {
                JsonObject o = object().put("testfield", RandomHelper.randomWord()).get();
                bi.index(o);
            }
        }
        client.refresh();
        SearchResponse iterableSearch = client.iterableSearch(index.index, index.type, QueryBuilder.query(0, 100, null), 2, 5, false);
        assertThat(iterableSearch.size()).isEqualTo(26);
    }

    public void shouldIndexAndDelete() throws IOException {
        RandomIndexHelper index = RandomIndexHelper.index();
        String idPrefix=HashUtils.createId();
        try (BulkIndexingOperations bi = client.bulkIndexer(index.index, index.type, 5, 1)) {
            for (int i = 0; i < 11; i++) {
                JsonObject o = object().put("id", idPrefix + "_" +i).get();
                bi.index(o);
            }
        }
        client.refresh();
        try (BulkIndexingOperations bi = client.bulkIndexer(index.index, index.type, 5, 1)) {
            for (int i = 0; i < 11; i++) {
                bi.delete(idPrefix + "_" +i);
            }
        }
        client.refresh();
        SearchResponse iterableSearch = client.iterableSearch(index.index, index.type, QueryBuilder.query(0, 100, null), 2, 5, false);
        assertThat(iterableSearch.size()).isEqualTo(0);
    }

    public void shouldIndexMultipleJsonObjectsWithoutExecutor() throws IOException {
        RandomIndexHelper index = RandomIndexHelper.index();
        try (BulkIndexingOperations bi = client.bulkIndexer(index.index, index.type, 5, 0)) {
            for (int i = 0; i < 26; i++) {
                JsonObject o = object().put("testfield", RandomHelper.randomWord()).get();
                bi.index(o);
            }
        }
        client.refresh();
        SearchResponse iterableSearch = client.iterableSearch(index.index, index.type, QueryBuilder.query(0, 100, null), 2, 5, false);
        assertThat(iterableSearch.size()).isEqualTo(26);
    }

    public void shouldIndexMultipleStrings() throws IOException {
        RandomIndexHelper index = RandomIndexHelper.index();
        try (BulkIndexingOperations bi = client.bulkIndexer(index.index, index.type, 5, 4)) {
            for (int i = 0; i < 26; i++) {
                JsonObject o = object().put("testfield", RandomHelper.randomWord()).get();
                bi.index(o);
            }
        }
        client.refresh();

        SearchResponse iterableSearch = client.iterableSearch(index.index, index.type, QueryBuilder.query(0, 100, null), 2, 5, false);
        assertThat(iterableSearch.size()).isEqualTo(26);
    }

    public void shouldReplaceExistingDocs() throws IOException {
        RandomIndexHelper index = RandomIndexHelper.index();
        try (BulkIndexingOperations bi = client.bulkIndexer(index.index, index.type, 5, 4)) {
            for (int i = 0; i < 26; i++) {
                bi.index(object(field("id", "" + i), field("testfield", "old")));
            }
        }

        try (BulkIndexingOperations bi = client.bulkIndexer(index.index, index.type, 5, 4)) {
            for (int i = 0; i < 26; i++) {
                bi.index(object(field("id", "" + i), field("testfield", "new_value")));
            }
        }
        client.refresh();

        String object = client.getObject(index.index, index.type, "12").toString();
        assertThat(object).contains("new_value");
    }

    public void shouldNotAllowVersionConflictDocuments() throws IOException {
        RandomIndexHelper index = RandomIndexHelper.index();
        try(BulkIndexingOperations bulkindexer = client.bulkIndexer(index.index, index.type, 5, 2)) {
            for (int i = 0; i < 20; i++) {
                bulkindexer.index(Integer.toString(i), index.type, null, null, object(field("id", "" + i), field("testfield", "old")));
            }
        }
        client.refresh();
        List<JsonObject> oldobjects = new ArrayList<>();
        for (int i=0; i < 20; i++) {
            oldobjects.add(client.getObject(index.index, index.type , Integer.toString(i)));
        }

        try(BulkIndexingOperations bulkindexer = client.bulkIndexer(index.index, index.type, 5, 2)) {
            for (JsonObject object2 : oldobjects) {
                String version = object2.getString("_version");
                String id = object2.getString("_id");
                bulkindexer.index(id, index.type, null, version, object(field("id", "" + id), field("testfield", "versionone")));
            }
        }
        client.refresh();

        try(BulkIndexingOperations bulkindexer = client.bulkIndexer(index.index, index.type, 5, 2)) {
            for (JsonObject object1 : oldobjects) {
                String version = object1.getString("_version");
                String id = object1.getString("_id");
                bulkindexer.index(id, index.type, null, version, object(field("id", "" + id), field("testfield", "conflictingversion")));
            }
        }
        client.refresh();

        String object = client.getObject(index.index, index.type, "12").toString();
        assertThat(object).contains("versionone");
    }

    public void shouldAllowUpdates() throws IOException {
        RandomIndexHelper index = RandomIndexHelper.index();
        try(BulkIndexingOperations bulkindexer = client.bulkIndexer(index.index, index.type, 5, 2)) {
            for (int i=0; i < 20; i++) {
                bulkindexer.index(Integer.toString(i), index.type, null, null, object(field("id", "" + i), field("testfield", "old")));
            }
        }
        client.refresh();
        List<JsonObject> oldobjects = new ArrayList<>();
        for (int i=0; i < 20; i++) {
            String object = client.getObject(index.index, index.type, Integer.toString(i)).toString();
            JsonObject a = (JsonObject) jsonparser.parseObject(object).get("_source");
            oldobjects.add(a);
        }

        try(BulkIndexer bulkindexer = client.bulkIndexer(index.index, index.type, 5, 2)) {
            for (JsonObject updateobject: oldobjects) {
                bulkindexer.update(updateobject.getString("id"), null, null, updateobject, o -> { o.put("testfield", "new"); return o; });
            }
            bulkindexer.flush();
        }
        client.refresh();
        assertThat(client.getObject(index.index, index.type, "12").toString()).contains("new");
    }
}
