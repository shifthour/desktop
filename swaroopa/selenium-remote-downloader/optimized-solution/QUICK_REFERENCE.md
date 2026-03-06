# Quick Reference Card

## Your Feature Line
```gherkin
Then I verify the remote file download and copy to the local path
```

## Installation (Copy-Paste)

### 1. Add to pom.xml
```xml
<!-- Selenium -->
<dependency>
    <groupId>org.seleniumhq.selenium</groupId>
    <artifactId>selenium-java</artifactId>
    <version>4.27.0</version>
</dependency>
<dependency>
    <groupId>org.seleniumhq.selenium</groupId>
    <artifactId>selenium-remote-driver</artifactId>
    <version>4.27.0</version>
</dependency>
<dependency>
    <groupId>org.seleniumhq.selenium</groupId>
    <artifactId>selenium-http</artifactId>
    <version>4.27.0</version>
</dependency>

<!-- Cucumber -->
<dependency>
    <groupId>io.cucumber</groupId>
    <artifactId>cucumber-java</artifactId>
    <version>7.15.0</version>
</dependency>
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

### 2. Copy Files to Your Project

**From optimized-solution, copy:**

```
RemoteFileDownloadUtil.java
  → to your: src/main/java/[yourpackage]/utils/

RemoteFileDownloadSteps.java
  → to your: src/test/java/[yourpackage]/steps/

Hooks.java
  → to your: src/test/java/[yourpackage]/hooks/
```

### 3. Update Package Names

In copied files, change:
```java
package com.automation.utils;  → package [yourpackage].utils;
package com.automation.steps;  → package [yourpackage].steps;
package com.automation.hooks;  → package [yourpackage].hooks;
```

### 4. Use in Feature File

```gherkin
Scenario: Download file
  Given I am on the download page
  When I click on the download button
  Then I verify the remote file download and copy to the local path
```

## Configuration

Create `config.properties`:
```properties
grid.url=http://localhost:4444/wd/hub
local.download.path=./downloads
download.timeout=60
```

## Run
```bash
mvn clean test
```

## Key Files in This Solution

| File | Purpose | Line # |
|------|---------|--------|
| `RemoteFileDownloadSteps.java` | Your step definition | Line 37 |
| `RemoteFileDownloadUtil.java` | Download utility | Line 47 |
| `Hooks.java` | WebDriver setup | Line 35 |
| `pom.xml` | Dependencies | Line 23 |

## Step Variations

```gherkin
# Basic
Then I verify the remote file download and copy to the local path

# Specific file
Then I verify the remote file download "report.pdf" and copy to the local path

# Custom timeout
Then I verify the remote file download and copy to the local path with timeout 120 seconds
```

## Common Commands

```bash
# Run all tests
mvn clean test

# Run with custom Grid
mvn clean test -Dgrid.url=http://remote:4444/wd/hub

# Run with custom path
mvn clean test -Dlocal.download.path=./mydownloads

# Run specific tag
mvn clean test -Dcucumber.filter.tags="@download"
```

## Customization Points

### Change Download Button Selector

Edit `NavigationSteps.java`:
```java
driver.findElement(By.id("download-button")).click();
// Change to your selector:
driver.findElement(By.xpath("//button[@class='download']")).click();
```

### Change URL

Edit `config.properties`:
```properties
# Or pass as system property:
mvn test -Ddownload.page.url=https://myapp.com/download
```

## Selenium Grid Setup

```bash
# Start Grid with downloads enabled
java -jar selenium-server-4.x.x.jar node \
  --enable-managed-downloads true
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| File not found | Increase timeout: `with timeout 120 seconds` |
| Connection refused | Check Grid URL in config.properties |
| Step not found | Update glue in TestRunner: `"yourpackage.steps"` |
| Driver null | Ensure Hooks.java is in glue package |

## File Locations

```
your-project/
├── src/main/java/[yourpackage]/utils/
│   └── RemoteFileDownloadUtil.java         ← Copy this
├── src/test/java/[yourpackage]/
│   ├── steps/
│   │   └── RemoteFileDownloadSteps.java    ← Copy this
│   └── hooks/
│       └── Hooks.java                       ← Copy this
├── src/test/resources/features/
│   └── YourTest.feature                     ← Use step here
├── pom.xml                                  ← Add dependencies
└── config.properties                        ← Create this
```

## That's It!

Three files to copy, one step to use:
```gherkin
Then I verify the remote file download and copy to the local path
```

Run with:
```bash
mvn clean test
```
