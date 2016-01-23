package io.inbot.elasticsearch.testutil;

import com.github.jillesvangurp.urlbuilder.UrlBuilder;
import java.util.Locale;

public class RandomIndexHelper {
    public String index = "test_" + RandomHelper.randomWord().toLowerCase(Locale.ENGLISH);
    public String type = "test_" + RandomHelper.randomWord().toLowerCase(Locale.ENGLISH);

    public String documentUrl(String id) {
        return UrlBuilder.url("/").append(index, type, "1").build();
    }

    public UrlBuilder url() {
        return UrlBuilder.url("/").append(index, type);
    }

    public static RandomIndexHelper index() {
        return new RandomIndexHelper();
    }
}