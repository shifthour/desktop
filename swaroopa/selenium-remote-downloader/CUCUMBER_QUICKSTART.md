# Cucumber Quick Start - File Download from Remote Grid

## What You Asked For

**Your Requirement:**
> "In my cucumber feature file, I will write: `Then verify file download on the remote machine`
> The step definition should call the Java file to download from remote Grid."

## ✅ Solution Ready!

Everything is implemented and ready to use. Just follow these steps:

## Step 1: Your Feature File

Create your feature file: `src/test/resources/features/your_test.feature`

```gherkin
Feature: Download File from Remote Grid

  Background:
    Given the remote file download is configured

  Scenario: Download and verify file
    Given I am on the download page
    When I click on download button
    Then verify file download on the remote machine
```

**That's it!** The step `Then verify file download on the remote machine` is already implemented.

## Step 2: Configure Settings

Edit `download.properties`:

```properties
# Choose download method
download.method=SELENIUM_4_API

# Where files will be saved on your local machine
local.download.path=./downloads

# Timeout for download
timeout.seconds=60
```

## Step 3: Run Tests

```bash
# Option 1: Using Maven
mvn clean test

# Option 2: Using the run script
./run-tests.sh

# Option 3: With custom Grid URL
mvn clean test -Dgrid.url=http://your-grid:4444/wd/hub
```

## How It Works

### 1. Feature File Step
```gherkin
Then verify file download on the remote machine
```

### 2. Step Definition (Already Written!)
**File:** `src/test/java/com/selenium/cucumber/steps/FileDownloadSteps.java:218`

```java
@Then("verify file download on the remote machine")
public void verifyFileDownloadOnTheRemoteMachine() {
    try {
        // 1. Downloads file from remote Grid
        String downloadedFile = downloadFileFromRemote("download");

        // 2. Verifies file exists on local machine
        File file = new File(downloadedFile);
        Assert.assertTrue(file.exists(), "Downloaded file should exist");
        Assert.assertTrue(file.length() > 0, "Downloaded file should not be empty");

        System.out.println("✓ File verified successfully: " + downloadedFile);
    } catch (Exception e) {
        Assert.fail("File download verification failed: " + e.getMessage());
    }
}
```

### 3. Java Downloader Classes (Automatically Called)

The step definition calls:
- **Selenium4Downloader.java** (for Selenium Grid 4+)
- **SSHFileTransfer.java** (for SSH/SCP method)

## All Available Steps

You can use these steps in your feature files:

### Basic Download
```gherkin
Then verify file download on the remote machine
```

### Specific Filename
```gherkin
Then verify file download on the remote machine with filename "report.pdf"
```

### File Extension
```gherkin
Then verify file download on the remote machine with extension ".xlsx"
```

### Custom Timeout
```gherkin
Then verify file download on the remote machine with timeout 300 seconds
```

### Multiple Files
```gherkin
When I download the following files:
  | invoice.pdf  |
  | receipt.pdf  |
  | summary.xlsx |
Then verify all files are downloaded on the remote machine
```

## Project Files Created

✅ **Feature File Template**
- `src/test/resources/features/file_download.feature`

✅ **Step Definitions** (Main File)
- `src/test/java/com/selenium/cucumber/steps/FileDownloadSteps.java`

✅ **Test Runner**
- `src/test/java/com/selenium/cucumber/TestRunner.java`

✅ **Hooks** (Setup/Teardown)
- `src/test/java/com/selenium/cucumber/hooks/TestHooks.java`

✅ **Shared State Manager**
- `src/test/java/com/selenium/cucumber/utils/TestContext.java`

✅ **Configuration**
- `download.properties`
- `testng.xml`

✅ **Dependencies**
- `pom.xml` (with Cucumber 7.15.0, TestNG 7.8.0, Selenium 4.27.0)

## Execution Flow

```
1. Feature File (your_test.feature)
   ↓
2. Step Definition (FileDownloadSteps.java)
   ↓
3. Downloader Class (Selenium4Downloader.java)
   ↓
4. Remote Selenium Grid
   ↓
5. File Downloaded to Local Machine (./downloads/)
   ↓
6. Verification (file exists & not empty)
   ↓
7. Test Pass/Fail
```

## Customize for Your App

Just update the element locators in `FileDownloadSteps.java`:

```java
@When("I click on download button")
public void iClickOnDownloadButton() {
    // Change this selector to match your app
    driver.findElement(By.id("download-button")).click();
}
```

## Run Your First Test

```bash
# 1. Build project
mvn clean install

# 2. Run tests
mvn clean test

# 3. View reports
open target/cucumber-reports/cucumber.html
```

## Example: Real-World Scenario

```gherkin
Feature: Monthly Report Download

  Background:
    Given the remote file download is configured

  @download @smoke
  Scenario: Download monthly sales report
    Given I am on the reports page
    When I generate monthly report
    And I click on download PDF button
    Then verify file download on the remote machine with filename "monthly-report.pdf"
```

**Step Definition:** Already implemented in `FileDownloadSteps.java`!

## Common Configuration

### For Local Grid
```bash
mvn clean test -Dgrid.url=http://localhost:4444/wd/hub
```

### For Remote Grid
```bash
mvn clean test -Dgrid.url=http://remote-grid.company.com:4444/wd/hub
```

### With SSH Method
In `download.properties`:
```properties
download.method=SSH_SCP
ssh.host=192.168.1.100
ssh.user=selenium
ssh.password=password
remote.download.path=/home/selenium/downloads
local.download.path=./downloads
```

## Troubleshooting

### Step definition not found?
Check `TestRunner.java:11`:
```java
glue = {"com.selenium.cucumber.steps", "com.selenium.cucumber.hooks"}
```

### File not downloading?
- Check Grid is running: `http://localhost:4444`
- Increase timeout: `with timeout 120 seconds`
- Check logs in `target/` folder

### Want to debug?
Run with verbose logging:
```bash
mvn clean test -X
```

## Next Steps

1. ✅ Copy this project to your workspace
2. ✅ Update `download.properties` with your Grid URL
3. ✅ Customize element locators in `FileDownloadSteps.java`
4. ✅ Write your feature files
5. ✅ Run: `mvn clean test`

## Full Documentation

- **Detailed Guide:** `CUCUMBER_GUIDE.md`
- **Main README:** `README.md`
- **Quick Start:** `QUICKSTART.md`

## That's It!

You now have a complete, optimized Cucumber BDD solution for downloading files from remote Selenium Grid to your local machine.

**Key Point:** Just use `Then verify file download on the remote machine` in your feature files, and everything is handled automatically!

Happy Testing! 🎉
