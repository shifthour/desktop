package com.selenium.cucumber;

import io.cucumber.testng.AbstractTestNGCucumberTests;
import io.cucumber.testng.CucumberOptions;
import org.testng.annotations.DataProvider;

/**
 * TestNG Cucumber Test Runner
 * Configure feature files, step definitions, and reporting options
 */
@CucumberOptions(
    features = "src/test/resources/features",              // Feature files location
    glue = {
        "com.selenium.cucumber.steps",                     // Step definitions package
        "com.selenium.cucumber.hooks"                      // Hooks package
    },
    tags = "@download",                                    // Run only download tests
    plugin = {
        "pretty",                                          // Console output
        "html:target/cucumber-reports/cucumber.html",      // HTML report
        "json:target/cucumber-reports/cucumber.json",      // JSON report
        "junit:target/cucumber-reports/cucumber.xml",      // JUnit XML report
        "com.aventstack.extentreports.cucumber.adapter.ExtentCucumberAdapter:" // Extent report
    },
    monochrome = true,                                     // Readable console output
    dryRun = false,                                        // Set to true to check step definitions
    publish = false                                        // Publish reports to Cucumber cloud
)
public class TestRunner extends AbstractTestNGCucumberTests {

    /**
     * Enable parallel execution
     * scenarios = true: Run scenarios in parallel
     */
    @Override
    @DataProvider(parallel = false)  // Set to true for parallel execution
    public Object[][] scenarios() {
        return super.scenarios();
    }
}
