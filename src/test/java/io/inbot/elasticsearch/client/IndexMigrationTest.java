package io.inbot.elasticsearch.client;

import static com.github.jsonj.tools.JsonBuilder.field;
import static com.github.jsonj.tools.JsonBuilder.object;
import static org.assertj.core.api.StrictAssertions.assertThat;

import io.inbot.elasticsearch.crud.CrudOperations;
import io.inbot.elasticsearch.crud.CrudOpererationsFactory;
import io.inbot.elasticsearch.testutil.DiyTestContext;
import io.inbot.elasticsearch.testutil.EsTestLauncher;
import io.inbot.elasticsearch.testutil.RandomHelper;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test
public class IndexMigrationTest {

    private EsAPIClient client;
    private CrudOpererationsFactory crudOperationsFactory;

    @BeforeMethod
    public void before() {
        EsTestLauncher.ensureEsIsUp();
        crudOperationsFactory = DiyTestContext.instance.crudOperationsFactory;
        client=DiyTestContext.instance.client;

    }

    public void shouldCreateAndThenMigrateIndex() {
        String indexName = RandomHelper.randomIndexName();
        ElasticSearchIndex index = ElasticSearchIndex.create(indexName, 1, "mapping-v1.json");
        ElasticSearchIndex index2 = ElasticSearchIndex.create(indexName, 2, "mapping-v1.json");
        assertThat(index.indexName()).isNotEqualTo(index2.indexName());
        CrudOperations dao = crudOperationsFactory.builder(ElasticSearchType.create(index, "test")).dao();
        client.migrateIndex(index);
        assertThat(client.indexExists(index.readAlias())).isTrue();
        assertThat(client.indexExists(index.writeAlias())).isTrue();
        assertThat(client.indexExists(index.indexName())).isTrue();
        client.refresh();
        for(int i=0;i<10;i++) {
            dao.create(object(field("id","obj_"+i)), true);
        }
        client.refresh();
        assertThat(dao.pagedSearch(QueryBuilder.query(QueryBuilder.matchAll()), 100, 0).size()).isEqualTo(10);
        client.verbose(false);
        client.migrateIndex(index2);
        client.refresh();
        assertThat(client.indexExists(index2.readAlias())).isTrue();
        assertThat(client.indexExists(index2.writeAlias())).isTrue();
        assertThat(client.indexExists(index2.indexName())).isTrue();
        System.out.println(index.indexName());
        System.out.println(client.getAliases().prettyPrint());
        assertThat(client.indexExists(index.indexName())).isFalse();
        assertThat(dao.pagedSearch(QueryBuilder.query(QueryBuilder.matchAll()), 100, 0).size()).isEqualTo(10);
    }

}
