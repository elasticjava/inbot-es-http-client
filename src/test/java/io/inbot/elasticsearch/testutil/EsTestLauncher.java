package io.inbot.elasticsearch.testutil;

import io.inbot.elasticsearch.launcher.ElasticSearchNodeHolder;
import org.apache.commons.lang3.RandomUtils;

public class EsTestLauncher {
    public static final String ES_URL="http://localhost:9222";

    private static InitializingAtomicReference<ElasticSearchNodeHolder> ref = new InitializingAtomicReference<>(() -> {
        ElasticSearchNodeHolder nodeholder = ElasticSearchNodeHolder.createWithDefaults("target/index_"+RandomUtils.nextInt(0, Integer.MAX_VALUE), 9222, false);
        nodeholder.start();
        return nodeholder;
    });

    public static void destroy() {
        ref.get().close();
    }


    public static void ensureEsIsUp() {
        ref.get();
    }
}
