# Optimized Remote File Download Solution

## Overview

This is an **optimized, production-ready solution** for downloading files from remote Selenium Grid to your local machine using Cucumber BDD.

### Your Feature Line
```gherkin
Then I verify the remote file download and copy to the local path
```

**Status:** ✅ **READY TO USE**

## Quick Start (3 Steps)

### 1. Add to Your Feature File

```gherkin
Feature: File Download Test

  @download
  Scenario: Download file from remote Grid
    Given I am on the download page
    When I click on the download button
    Then I verify the remote file download and copy to the local path
```

### 2. Configure

Edit `config.properties`:
```properties
grid.url=http://localhost:4444/wd/hub
local.download.path=./downloads
download.timeout=60
```

### 3. Run

```bash
mvn clean test
```

**That's it!** ✅

## Project Structure

```
optimized-solution/
├── src/
│   ├── main/java/com/automation/utils/
│   │   └── RemoteFileDownloadUtil.java    ⭐ Main utility class
│   │
│   └── test/java/com/automation/
│       ├── TestRunner.java                 ⭐ Test runner
│       ├── hooks/
│       │   └── Hooks.java                  ⭐ WebDriver setup/teardown
│       └── steps/
│           ├── RemoteFileDownloadSteps.java ⭐ YOUR STEP DEFINITION
│           └── NavigationSteps.java         Supporting steps
│
├── src/test/resources/features/
│   └── RemoteFileDownload.feature          ⭐ Example feature file
│
├── pom.xml                                  ⭐ Maven dependencies
├── config.properties                        Configuration
└── README.md                                This file
```

## Key Files

### 1. Step Definition (YOUR REQUESTED FILE)

**File:** `src/test/java/com/automation/steps/RemoteFileDownloadSteps.java`

```java
@Then("I verify the remote file download and copy to the local path")
public void iVerifyTheRemoteFileDownloadAndCopyToTheLocalPath() {
    // Downloads file from remote Grid to local path
    String downloadedFilePath = fileDownloadUtil.downloadAndVerifyFile();

    // Verifies file exists locally
    Assert.assertTrue(new File(downloadedFilePath).exists());

    System.out.println("✓ SUCCESS: File downloaded to " + downloadedFilePath);
}
```

### 2. Utility Class (THE WORKER)

**File:** `src/main/java/com/automation/utils/RemoteFileDownloadUtil.java`

**Key Methods:**
- `downloadAndVerifyFile()` - Downloads latest file from Grid
- `downloadAndVerifyFile(String fileName)` - Downloads specific file
- Uses Selenium Grid 4+ API endpoint: `/session/{sessionId}/se/files`

**How it works:**
1. Polls Selenium Grid for downloadable files
2. Finds the file (by name or latest)
3. Downloads file content via HTTP
4. Saves to local path
5. Returns local file path

### 3. POM.xml Dependencies

**File:** `pom.xml`

**Required dependencies:**
```xml
<!-- Selenium Java -->
<dependency>
    <groupId>org.seleniumhq.selenium</groupId>
    <artifactId>selenium-java</artifactId>
    <version>4.27.0</version>
</dependency>

<!-- Selenium Remote Driver -->
<dependency>
    <groupId>org.seleniumhq.selenium</groupId>
    <artifactId>selenium-remote-driver</artifactId>
    <version>4.27.0</version>
</dependency>

<!-- Selenium HTTP Client -->
<dependency>
    <groupId>org.seleniumhq.selenium</groupId>
    <artifactId>selenium-http</artifactId>
    <version>4.27.0</version>
</dependency>

<!-- Cucumber Java -->
<dependency>
    <groupId>io.cucumber</groupId>
    <artifactId>cucumber-java</artifactId>
    <version>7.15.0</version>
</dependency>

<!-- Cucumber TestNG -->
<dependency>
    <groupId>io.cucumber</groupId>
    <artifactId>cucumber-testng</artifactId>
    <version>7.15.0</version>
</dependency>

<!-- TestNG -->
<dependency>
    <groupId>org.testng</groupId>
    <artifactId>testng</artifactId>
    <version>7.8.0</version>
</dependency>
```

## Available Step Variations

### Basic (Auto-detects file)
```gherkin
Then I verify the remote file download and copy to the local path
```

### Specific Filename
```gherkin
Then I verify the remote file download "report.pdf" and copy to the local path
```

### Custom Timeout (for large files)
```gherkin
Then I verify the remote file download and copy to the local path with timeout 120 seconds
```

## Configuration Options

### Via config.properties
```properties
grid.url=http://localhost:4444/wd/hub
local.download.path=./downloads
download.timeout=60
remote.download.path=/tmp/downloads
headless=true
```

### Via System Properties
```bash
mvn clean test \
  -Dgrid.url=http://remote-grid:4444/wd/hub \
  -Dlocal.download.path=./test-downloads \
  -Ddownload.timeout=120
```

### Via Code (if needed)
```java
RemoteFileDownloadUtil util = new RemoteFileDownloadUtil(
    driver,
    "./downloads",  // local path
    60              // timeout seconds
);
```

## How It Works (Flow)

```
1. Feature File
   "Then I verify the remote file download and copy to the local path"
   ↓
2. Step Definition (RemoteFileDownloadSteps.java:37)
   iVerifyTheRemoteFileDownloadAndCopyToTheLocalPath()
   ↓
3. Utility Class (RemoteFileDownloadUtil.java:47)
   downloadAndVerifyFile()
   ↓
4. Selenium Grid API
   GET /session/{sessionId}/se/files  (list files)
   GET /session/{sessionId}/se/files/{fileName}  (download)
   ↓
5. File Downloaded
   Saved to: ./downloads/filename.pdf
   ↓
6. Verification
   File exists ✓
   File not empty ✓
   ↓
7. Test PASS
```

## Running Tests

### Basic Run
```bash
mvn clean test
```

### With Custom Configuration
```bash
mvn clean test -Dconfig.file=myconfig.properties
```

### Run Specific Tag
```bash
mvn clean test -Dcucumber.filter.tags="@download"
```

### With Custom Grid
```bash
mvn clean test -Dgrid.url=http://remote-grid.company.com:4444/wd/hub
```

## Integration with Your Project

### Option 1: Copy Files Directly

1. Copy `RemoteFileDownloadUtil.java` to your utils package
2. Copy `RemoteFileDownloadSteps.java` to your steps package
3. Add POM dependencies from `pom.xml`
4. Use the step in your feature files

### Option 2: Build as Dependency

```bash
cd optimized-solution
mvn clean install
```

Then in your project's `pom.xml`:
```xml
<dependency>
    <groupId>com.automation</groupId>
    <artifactId>remote-file-download</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Customization

### Change Element Locators

Edit `NavigationSteps.java` to match your application:

```java
@When("I click on the download button")
public void iClickOnTheDownloadButton() {
    // Change this to match your app's selector
    driver.findElement(By.id("download-button")).click();
}
```

### Add Custom Steps

Add to `RemoteFileDownloadSteps.java`:

```java
@Then("I verify the file size is greater than {int} bytes")
public void iVerifyFileSize(int minSize) {
    String filePath = fileDownloadUtil.getLastDownloadedFile();
    File file = new File(filePath);
    Assert.assertTrue(file.length() > minSize);
}
```

## Requirements

- **Java:** 11 or higher
- **Maven:** 3.6+
- **Selenium Grid:** 4.0+ (with `--enable-managed-downloads true`)
- **Browser:** Chrome/Edge (configured on Grid)

## Selenium Grid Setup

### Start Grid with Download Support

```bash
# Start Hub
java -jar selenium-server-4.x.x.jar hub

# Start Node with downloads enabled
java -jar selenium-server-4.x.x.jar node \
  --enable-managed-downloads true \
  --selenium-manager true
```

## Advantages of This Solution

✅ **Simple** - One line in feature file
✅ **Optimized** - Uses Selenium Grid 4 API (no SSH needed)
✅ **Production-Ready** - Error handling, timeouts, logging
✅ **Cross-Platform** - Works on Windows, Mac, Linux
✅ **Configurable** - Multiple configuration options
✅ **Reusable** - Can be used across multiple projects
✅ **Clean Code** - Well-structured, maintainable
✅ **Thread-Safe** - Supports parallel execution

## Troubleshooting

### Issue: File not found in Grid

**Solution:**
- Ensure Grid is version 4.0+
- Check Grid node started with `--enable-managed-downloads true`
- Increase timeout: `with timeout 120 seconds`

### Issue: Connection refused

**Solution:**
- Verify Grid is running: `http://localhost:4444`
- Check `grid.url` in config.properties
- Test Grid in browser

### Issue: Step definition not found

**Solution:**
- Check `TestRunner.java` glue packages:
  ```java
  glue = {"com.automation.steps", "com.automation.hooks"}
  ```

### Issue: Driver is null

**Solution:**
- Ensure `Hooks.java` `@Before` method runs
- Check TestNG/Cucumber execution order
- Verify `Hooks.getDriver()` is accessible

## Example Test Run

```bash
$ mvn clean test

========================================
Starting Scenario: Verify remote file download and copy to local path
========================================
✓ WebDriver initialized successfully
  - Grid URL: http://localhost:4444/wd/hub
  - Session ID: abc123...
  - Remote Download Path: /tmp/downloads
  - Local Download Path: ./downloads

Navigated to download page: https://example.com/download
Clicked download button
Starting remote file download verification...
RemoteFileDownloadUtil initialized:
  - Session ID: abc123...
  - Grid URL: http://localhost:4444
  - Local Path: ./downloads
  - Timeout: 60 seconds

Waiting for file in remote Grid...
Available files in Grid: [report-2025.pdf]
✓ Found file: report-2025.pdf
Downloading file from Grid: report-2025.pdf
✓ Downloaded 125634 bytes from Grid
✓ File saved to local path: /path/to/downloads/report-2025.pdf
✓ File successfully downloaded from remote Grid

✓ SUCCESS: File downloaded and verified
  - Local Path: /path/to/downloads/report-2025.pdf
  - File Size: 125634 bytes
  - File Name: report-2025.pdf

✓ WebDriver closed
========================================
Scenario: Verify remote file download and copy to local path - PASSED
========================================

Tests run: 1, Failures: 0, Errors: 0, Skipped: 0
```

## Support

For issues or questions:
1. Check Grid logs: `http://localhost:4444/ui`
2. Enable debug logging: `-X` flag with Maven
3. Review step definition: `RemoteFileDownloadSteps.java:37`
4. Review utility class: `RemoteFileDownloadUtil.java:47`

## Summary

This is a **complete, optimized solution** for your requirement:

**Feature Line:** `Then I verify the remote file download and copy to the local path`

**Files Provided:**
1. ✅ Step Definition: `RemoteFileDownloadSteps.java`
2. ✅ Utility Class: `RemoteFileDownloadUtil.java`
3. ✅ POM Dependencies: `pom.xml`
4. ✅ Configuration: `config.properties`
5. ✅ Hooks: `Hooks.java`
6. ✅ Test Runner: `TestRunner.java`
7. ✅ Example Feature: `RemoteFileDownload.feature`

**Just run:** `mvn clean test` and you're done! 🎉
