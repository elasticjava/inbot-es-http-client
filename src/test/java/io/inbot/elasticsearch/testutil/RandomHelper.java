package io.inbot.elasticsearch.testutil;

import io.inbot.testfixtures.RandomNameGenerator;
import java.util.Locale;
import java.util.Random;
import org.apache.commons.lang3.StringUtils;

public class RandomHelper {
    public static final long TESTSEED;

    private static final Random RANDOM;

    private static final RandomNameGenerator randomNameGenerator;

    private static RandomStringUtilsWithSeed randomStringUtilsWithSeed;

    static {
        long seed = System.nanoTime();
        String customSeed = System.getProperty("inbotTestSeed");
        if(StringUtils.isNotBlank(customSeed)) {
            seed = Long.valueOf(customSeed);
        }
        // TODO use logger
        System.err.println(String.format(Locale.ENGLISH, "\n\nTest random seed: %s. If test fails reproduce order by passing in -DinbotTestSeed=%s\n\n", seed,
                seed));
        TESTSEED = seed;
        RANDOM = new Random(TESTSEED);
        randomNameGenerator = new RandomNameGenerator(TESTSEED);
        randomStringUtilsWithSeed = new RandomStringUtilsWithSeed(TESTSEED);
    }

    public static String randomWord() {

        String word = randomStringUtilsWithSeed.randomAlphanumeric(5 + RANDOM.nextInt(4));
        while (word.toLowerCase(Locale.ENGLISH).indexOf("null") >= 0) {
            word = randomStringUtilsWithSeed.randomAlphanumeric(5 + RANDOM.nextInt(4));
        }
        return word;
    }

}
