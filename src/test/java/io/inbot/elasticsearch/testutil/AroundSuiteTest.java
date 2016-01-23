package io.inbot.elasticsearch.testutil;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

public class AroundSuiteTest {
    @BeforeSuite
    public void beforeAll() {
        // ensures that we have everything running before the tests run in maven.
        EsTestLauncher.ensureEsIsUp();
    }

    @AfterSuite
    public void afterAll() {
    }
}
