# Cucumber BDD Integration Guide

Complete guide for using the Remote File Downloader with Cucumber BDD framework.

## Quick Start

### 1. Run Tests

```bash
# Run all download tests
mvn clean test

# Run with TestNG XML
mvn clean test -DsuiteXmlFile=testng.xml

# Run specific tags
mvn clean test -Dcucumber.filter.tags="@download and @smoke"

# Run with custom Grid URL
mvn clean test -Dgrid.url=http://remote-grid:4444/wd/hub
```

### 2. Feature File Example

The simplest step you need:

```gherkin
Feature: File Download Verification

  Scenario: Download file from remote Grid
    Given the remote file download is configured
    And I am on the download page
    When I click on download button
    Then verify file download on the remote machine
```

That's it! The step definition handles everything automatically.

## Available Steps

### Setup Steps

```gherkin
Given the remote file download is configured
Given I am on the download page
Given I am on the reports page
Given I am on the documents page
Given I am on the export page
```

### Action Steps

```gherkin
When I click on download button
When I click on download PDF button
When I generate monthly report
When I select "PDF" export option
When I click on export button
When I download large dataset file
When I download the following files:
  | invoice.pdf  |
  | receipt.pdf  |
  | summary.xlsx |
```

### Verification Steps (Main)

```gherkin
# Basic verification - auto-detects any downloaded file
Then verify file download on the remote machine

# Verify specific filename
Then verify file download on the remote machine with filename "report.pdf"

# Verify file extension
Then verify file download on the remote machine with extension ".xlsx"

# Verify with custom timeout (for large files)
Then verify file download on the remote machine with timeout 300 seconds

# Verify multiple files
Then verify all files are downloaded on the remote machine

# Negative test
Then verify download fails gracefully with error message
```

## Step Definition Implementation

Here's how the main verification step works:

**FileDownloadSteps.java:218**

```java
@Then("verify file download on the remote machine")
public void verifyFileDownloadOnTheRemoteMachine() {
    try {
        // Downloads file from remote Grid to local machine
        String downloadedFile = downloadFileFromRemote("download");

        // Verifies file exists and is not empty
        File file = new File(downloadedFile);
        Assert.assertTrue(file.exists(), "Downloaded file should exist");
        Assert.assertTrue(file.length() > 0, "Downloaded file should not be empty");

        System.out.println("✓ File verified successfully: " + downloadedFile);
    } catch (Exception e) {
        Assert.fail("File download verification failed: " + e.getMessage());
    }
}
```

## Configuration

### Option 1: Using download.properties

Create `download.properties` in project root:

```properties
# Download method
download.method=SELENIUM_4_API

# Local path where files will be saved
local.download.path=./downloads

# Timeout in seconds
timeout.seconds=60

# For SSH method (if using SSH_SCP)
# ssh.host=192.168.1.100
# ssh.user=selenium
# ssh.password=password
# remote.download.path=/home/selenium/downloads
```

### Option 2: Using System Properties

```bash
mvn clean test \
  -Dlocal.download.path=./test-downloads \
  -Ddownload.timeout=120 \
  -Dgrid.url=http://grid:4444/wd/hub
```

### Option 3: In Code (TestHooks.java)

Modify `TestHooks.java:37` to set defaults:

```java
String gridUrl = System.getProperty("grid.url", "http://localhost:4444/wd/hub");
String remoteDownloadPath = System.getProperty("remote.download.path", "/tmp/downloads");
```

## Complete Examples

### Example 1: Simple PDF Download

**Feature File:**
```gherkin
@download
Scenario: Download monthly report
  Given the remote file download is configured
  And I am on the reports page
  When I generate monthly report
  And I click on download PDF button
  Then verify file download on the remote machine with filename "monthly-report.pdf"
```

**Your Step Definition (if you need custom actions):**
```java
// FileDownloadSteps.java already has these implemented!
// Just use them directly in your feature file
```

### Example 2: Multiple Files Download

**Feature File:**
```gherkin
@download
Scenario: Download multiple documents
  Given the remote file download is configured
  And I am on the documents page
  When I download the following files:
    | invoice.pdf    |
    | receipt.pdf    |
    | summary.xlsx   |
  Then verify all files are downloaded on the remote machine
```

### Example 3: Different Export Formats

**Feature File:**
```gherkin
@download
Scenario Outline: Export in different formats
  Given the remote file download is configured
  And I am on the export page
  When I select "<format>" export option
  And I click on export button
  Then verify file download on the remote machine with extension "<extension>"

  Examples:
    | format | extension |
    | PDF    | .pdf      |
    | Excel  | .xlsx     |
    | CSV    | .csv      |
```

### Example 4: Large File with Custom Timeout

**Feature File:**
```gherkin
@download @timeout
Scenario: Download large dataset
  Given the remote file download is configured
  And I am on the download page
  When I download large dataset file
  Then verify file download on the remote machine with timeout 300 seconds
```

## Adding Custom Steps

### Option 1: Add to FileDownloadSteps.java

```java
@When("I click on {string} download link")
public void iClickOnDownloadLink(String linkText) {
    driver.findElement(By.linkText(linkText)).click();
}

@Then("verify file {string} is downloaded with size greater than {int} bytes")
public void verifyFileSize(String fileName, int minSize) {
    String filePath = downloadFileFromRemote(fileName);
    File file = new File(filePath);
    Assert.assertTrue(file.length() > minSize,
        "File size should be > " + minSize);
}
```

### Option 2: Create New Step Definition File

```java
package com.selenium.cucumber.steps;

import com.selenium.cucumber.utils.TestContext;
import io.cucumber.java.en.When;
import org.openqa.selenium.By;

public class CustomDownloadSteps {

    private final TestContext context;

    public CustomDownloadSteps() {
        this.context = TestContext.getInstance();
    }

    @When("I download report for month {string}")
    public void downloadMonthlyReport(String month) {
        context.getDriver().findElement(By.id("month-select"))
               .sendKeys(month);
        context.getDriver().findElement(By.id("download-report"))
               .click();
    }
}
```

## Project Structure

```
selenium-remote-downloader/
├── src/test/
│   ├── java/com/selenium/cucumber/
│   │   ├── TestRunner.java              # Main test runner
│   │   ├── steps/
│   │   │   └── FileDownloadSteps.java   # All step definitions
│   │   ├── hooks/
│   │   │   └── TestHooks.java           # Setup/teardown
│   │   └── utils/
│   │       └── TestContext.java         # Shared state
│   └── resources/
│       └── features/
│           └── file_download.feature    # Your feature files
├── download.properties                   # Configuration
└── testng.xml                           # TestNG config
```

## Key Files Reference

1. **TestRunner.java:1** - Configure test execution, tags, reports
2. **FileDownloadSteps.java:29** - All step definitions
3. **TestHooks.java:15** - WebDriver setup and teardown
4. **TestContext.java:12** - Share data between steps
5. **file_download.feature:1** - Feature file template

## Running Tests

### IDE (IntelliJ/Eclipse)

1. Right-click on `TestRunner.java`
2. Select "Run as TestNG Test"

### Command Line

```bash
# All tests
mvn clean test

# Specific tag
mvn clean test -Dcucumber.filter.tags="@smoke"

# With custom properties
mvn clean test \
  -Dgrid.url=http://grid:4444/wd/hub \
  -Dlocal.download.path=./downloads \
  -Dheadless=true
```

### CI/CD (Jenkins/GitHub Actions)

```yaml
# .github/workflows/test.yml
name: Cucumber Tests
on: [push]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up JDK 11
        uses: actions/setup-java@v2
        with:
          java-version: '11'
      - name: Run tests
        run: mvn clean test -Dgrid.url=${{ secrets.GRID_URL }}
```

## Reports

After test execution, reports are generated at:

- **HTML Report:** `target/cucumber-reports/cucumber.html`
- **JSON Report:** `target/cucumber-reports/cucumber.json`
- **Extent Report:** `test-output/ExtentReports/ExtentReport.html`

Open in browser:
```bash
open target/cucumber-reports/cucumber.html
```

## Troubleshooting

### Issue: Step definition not found

**Solution:** Check glue path in `TestRunner.java:11`:
```java
glue = {
    "com.selenium.cucumber.steps",
    "com.selenium.cucumber.hooks"
}
```

### Issue: Driver not initialized

**Solution:** Ensure `@Before` hook runs:
- Check `TestHooks.java:33`
- Verify Grid URL is accessible
- Check logs for connection errors

### Issue: File not found within timeout

**Solution:**
- Increase timeout in step: `with timeout 120 seconds`
- Or in properties: `timeout.seconds=120`
- Verify file actually downloads on remote machine

### Issue: Configuration not loaded

**Solution:**
- Ensure `download.properties` exists in project root
- Or pass system properties: `-Dlocal.download.path=./downloads`
- Check logs for "Loaded config from:" message

## Best Practices

1. **Use meaningful scenario names**
   ```gherkin
   ✓ Scenario: Download quarterly sales report as PDF
   ✗ Scenario: Test download
   ```

2. **Tag your scenarios**
   ```gherkin
   @download @smoke @regression
   Scenario: ...
   ```

3. **Use Background for common steps**
   ```gherkin
   Background:
     Given the remote file download is configured
     And I am logged in as admin
   ```

4. **Parameterize with Examples**
   ```gherkin
   Scenario Outline: Download <format> report
     When I select "<format>" format
     Then verify file download with extension "<ext>"
     Examples:
       | format | ext   |
       | PDF    | .pdf  |
       | Excel  | .xlsx |
   ```

5. **Clean up after tests**
   ```gherkin
   @cleanup  # Automatically deletes downloaded files
   Scenario: ...
   ```

## Advanced Usage

### Parallel Execution

In `TestRunner.java:35`:
```java
@DataProvider(parallel = true)  // Enable parallel execution
public Object[][] scenarios() {
    return super.scenarios();
}
```

### Custom Download Method

In your step definition:
```java
@Given("the remote file download is configured with SSH")
public void configureSSH() {
    DownloadConfig config = new DownloadConfig.Builder()
        .downloadMethod(DownloadConfig.DownloadMethod.SSH_SCP)
        .sshHost("192.168.1.100")
        .sshUser("selenium")
        .sshPassword("password")
        .build();
    context.setConfig(config);
    // Initialize SSH transfer
}
```

### File Validation

Add custom verification:
```java
@Then("verify PDF file is valid")
public void verifyPDFValidity() {
    String filePath = context.getLastDownloadedFile();
    // Add PDF validation logic
    PDDocument pdf = PDDocument.load(new File(filePath));
    Assert.assertTrue(pdf.getNumberOfPages() > 0);
    pdf.close();
}
```

## Next Steps

1. Customize element locators in `FileDownloadSteps.java` for your app
2. Add your own feature files in `src/test/resources/features/`
3. Configure Grid URL and download paths
4. Run your first test: `mvn clean test`

## Support

- Feature file: `src/test/resources/features/file_download.feature`
- Step definitions: `src/test/java/com/selenium/cucumber/steps/FileDownloadSteps.java`
- Main README: `README.md`
