package com.automation;

import io.cucumber.testng.AbstractTestNGCucumberTests;
import io.cucumber.testng.CucumberOptions;
import org.testng.annotations.DataProvider;

/**
 * TestNG Cucumber Test Runner
 */
@CucumberOptions(
    features = "src/test/resources/features",
    glue = {
        "com.automation.steps",
        "com.automation.hooks"
    },
    tags = "@download",
    plugin = {
        "pretty",
        "html:target/cucumber-reports/cucumber-report.html",
        "json:target/cucumber-reports/cucumber.json",
        "junit:target/cucumber-reports/cucumber.xml"
    },
    monochrome = true,
    dryRun = false
)
public class TestRunner extends AbstractTestNGCucumberTests {

    @Override
    @DataProvider(parallel = false)
    public Object[][] scenarios() {
        return super.scenarios();
    }
}
