package io.inbot.elasticsearch.jsonclient;

import static com.github.jillesvangurp.urlbuilder.UrlBuilder.url;
import static com.github.jsonj.tools.JsonBuilder.field;
import static com.github.jsonj.tools.JsonBuilder.object;
import static org.assertj.core.api.StrictAssertions.assertThat;

import com.github.jsonj.JsonObject;
import io.inbot.elasticsearch.exceptions.EsVersionConflictException;
import io.inbot.elasticsearch.testutil.EsTestLauncher;
import io.inbot.elasticsearch.testutil.TestFixture;
import java.util.Optional;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test
public class JsonJRestClientTest {

    private String index;
    private String type;
    private JsonJRestClient client;
    private String documentUrl;
    private JsonObject sampleJson;

    @BeforeMethod
    public void before() {
        EsTestLauncher.ensureEsIsUp();
        client = JsonJRestClient.simpleClient(EsTestLauncher.ES_URL);
        client.setVerbose(true);
        index = "bulkindexertest";
        type = "test-"+TestFixture.randomWord();
        documentUrl = url("/").append(index,type,"1").build();
        sampleJson = object(field("message","hello wrld"));
    }

    public void shouldCreateDocumentAndReturnTheDocumentWithAVersionAndId() {
        Optional<JsonObject> maybeDocument = client.post(url("/").append(index,type,"1").build(), sampleJson);
        JsonObject response = maybeDocument.get(); // would throw exception in case there was a 404 on elasticsearch
        assertThat(response.getInt("_version")).isEqualTo(1);
        assertThat(response.get("created",false)).isTrue();
    }

    public void shouldGetDocumentById() {
        client.put(documentUrl, sampleJson);
        JsonObject document = client.get(documentUrl).get();
        assertThat(document.getString("_source","message")).isEqualTo(sampleJson.getString("message"));
    }

    public void shouldDeleteDocument() {
        client.put(documentUrl, sampleJson);
        JsonObject response = client.delete(documentUrl).get();
        assertThat(response.getInt("_shards","successful")).isEqualTo(1); // es reported successful deletion
    }

    public void shouldReturnOptionalNotPresentOnES404() {
        assertThat(client.get(documentUrl).isPresent()).isFalse();
    }

    @Test(expectedExceptions=EsVersionConflictException.class)
    public void shouldThrowVersionConflict() {
        client.put(documentUrl, sampleJson);
        client.put(url(documentUrl).queryParam("version", 666).build(), sampleJson);
    }
}
