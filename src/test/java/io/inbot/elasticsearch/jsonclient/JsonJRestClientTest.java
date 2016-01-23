package io.inbot.elasticsearch.jsonclient;

import static com.github.jsonj.tools.JsonBuilder.field;
import static com.github.jsonj.tools.JsonBuilder.object;
import static org.assertj.core.api.StrictAssertions.assertThat;

import com.github.jsonj.JsonObject;
import io.inbot.elasticsearch.exceptions.EsVersionConflictException;
import io.inbot.elasticsearch.testutil.EsTestLauncher;
import io.inbot.elasticsearch.testutil.RandomIndexHelper;
import java.util.Optional;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test
public class JsonJRestClientTest {

    private JsonJRestClient client;
    private JsonObject sampleJson;

    @BeforeMethod
    public void before() {
        EsTestLauncher.ensureEsIsUp();
        client = JsonJRestClient.simpleClient(EsTestLauncher.ES_URL);
        client.setVerbose(false);
        sampleJson = object(field("message","hello wrld"));
    }

    public void shouldCreateDocumentAndReturnTheDocumentWithAVersionAndId() {
        RandomIndexHelper index = RandomIndexHelper.index();

        Optional<JsonObject> maybeDocument = client.post(index.documentUrl("1"), sampleJson);
        JsonObject response = maybeDocument.get(); // would throw exception in case there was a 404 on elasticsearch
        assertThat(response.getInt("_version")).isEqualTo(1);
        assertThat(response.get("created",false)).isTrue();
    }

    public void shouldGetDocumentById() {
        RandomIndexHelper index = RandomIndexHelper.index();
        client.put(index.documentUrl("1"), sampleJson);
        JsonObject document = client.get(index.documentUrl("1")).get();
        assertThat(document.getString("_source","message")).isEqualTo(sampleJson.getString("message"));
    }

    public void shouldDeleteDocument() {
        RandomIndexHelper index = RandomIndexHelper.index();

        client.put(index.documentUrl("1"), sampleJson);
        JsonObject response = client.delete(index.documentUrl("1")).get();
        assertThat(response.getInt("_shards","successful")).isEqualTo(1); // es reported successful deletion
    }

    public void shouldReturnOptionalNotPresentOnES404() {
        RandomIndexHelper index = RandomIndexHelper.index();

        assertThat(client.get(index.documentUrl("1")).isPresent()).isFalse();
    }

    @Test(expectedExceptions=EsVersionConflictException.class)
    public void shouldThrowVersionConflict() {
        RandomIndexHelper index = RandomIndexHelper.index();

        client.put(index.documentUrl("1"), sampleJson);
        client.put(index.url().append("1").queryParam("version", 666).build(), sampleJson);
    }
}
