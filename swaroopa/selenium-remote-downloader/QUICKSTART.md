# Quick Start Guide

## 1. Choose Your Approach

### Option A: Selenium 4 API (EASIEST - Recommended)

**Requirements:** Selenium Grid 4+

```java
import com.selenium.downloader.Selenium4Downloader;
import org.openqa.selenium.By;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.remote.RemoteWebDriver;
import java.net.URL;

// 1. Setup driver with download preferences
ChromeOptions options = new ChromeOptions();
Map<String, Object> prefs = new HashMap<>();
prefs.put("download.default_directory", "/tmp/downloads");
options.setExperimentalOption("prefs", prefs);

RemoteWebDriver driver = new RemoteWebDriver(
    new URL("http://your-grid-url:4444/wd/hub"),
    options
);

// 2. Create downloader
Selenium4Downloader downloader = new Selenium4Downloader(
    "/path/to/local/downloads",  // Where to save files locally
    60                            // Timeout in seconds
);

// 3. In your test, trigger download
driver.get("https://example.com");
driver.findElement(By.id("download-button")).click();

// 4. Download file to local machine
String localFilePath = downloader.downloadFile(driver, "filename.pdf");
System.out.println("File saved at: " + localFilePath);

driver.quit();
```

### Option B: SSH/SCP Transfer

**Requirements:** SSH access to Grid node

```java
import com.selenium.downloader.SSHFileTransfer;

// 1. Configure SSH transfer
SSHFileTransfer transfer = new SSHFileTransfer.Builder()
    .remoteHost("192.168.1.100")              // Grid node IP/hostname
    .remoteUser("username")
    .remotePassword("password")               // Or use .privateKeyPath()
    .remoteDownloadPath("/home/user/downloads")
    .localDownloadPath("./downloads")
    .timeoutSeconds(60)
    .build();

// 2. After download triggered in test
String localFile = transfer.downloadFile("report.pdf");
System.out.println("File transferred to: " + localFile);
```

## 2. Add to Your Maven Project

Add dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>com.selenium</groupId>
    <artifactId>remote-file-downloader</artifactId>
    <version>1.0.0</version>
</dependency>
```

Or build from source:

```bash
cd selenium-remote-downloader
mvn clean install
```

## 3. Common Patterns

### Pattern 1: Download and Verify

```java
Selenium4Downloader downloader = new Selenium4Downloader("./downloads", 60);

// Trigger download
driver.findElement(By.linkText("Download Report")).click();

// Wait and download
String filePath = downloader.downloadFile(driver, "report.pdf");

// Verify file exists
File file = new File(filePath);
assert file.exists() : "File not downloaded!";
assert file.length() > 0 : "File is empty!";

System.out.println("✓ File downloaded and verified");
```

### Pattern 2: Multiple Files

```java
String[] files = {"invoice.pdf", "receipt.pdf", "summary.xlsx"};

for (String fileName : files) {
    // Trigger download for each file
    driver.findElement(By.id("download-" + fileName)).click();

    // Download to local
    String path = downloader.downloadFile(driver, fileName);
    System.out.println("Downloaded: " + path);
}
```

### Pattern 3: With Configuration File

Create `download.properties`:
```properties
local.download.path=./downloads
download.method=SELENIUM_4_API
timeout.seconds=60
```

Use in code:
```java
DownloadConfig config = DownloadConfig.fromPropertiesFile("download.properties");

Selenium4Downloader downloader = new Selenium4Downloader(
    config.getLocalDownloadPath(),
    config.getTimeoutSeconds()
);
```

## 4. Troubleshooting

### Problem: Timeout waiting for file

**Solutions:**
- Increase timeout: `new Selenium4Downloader("./downloads", 120)`
- Verify file is actually downloading on remote machine
- Check remote download path in Chrome options

### Problem: SSH connection failed

**Solutions:**
- Test SSH manually: `ssh username@grid-node-ip`
- Check firewall rules
- Verify credentials

### Problem: File not found

**Solutions:**
- Use partial filename: `downloader.downloadFile(driver, "report")` instead of full name
- Add wait before download: `Thread.sleep(2000)`
- Check Grid logs for errors

## 5. Real Test Example

```java
import org.testng.annotations.*;
import com.selenium.downloader.Selenium4Downloader;

public class DownloadTest {

    RemoteWebDriver driver;
    Selenium4Downloader downloader;

    @BeforeClass
    public void setup() throws Exception {
        ChromeOptions options = new ChromeOptions();
        Map<String, Object> prefs = new HashMap<>();
        prefs.put("download.default_directory", "/tmp/downloads");
        options.setExperimentalOption("prefs", prefs);

        driver = new RemoteWebDriver(
            new URL("http://localhost:4444/wd/hub"),
            options
        );

        downloader = new Selenium4Downloader("./test-downloads", 60);
    }

    @Test
    public void testDownloadReport() throws Exception {
        // Navigate to app
        driver.get("https://myapp.com/reports");

        // Login
        driver.findElement(By.id("username")).sendKeys("testuser");
        driver.findElement(By.id("password")).sendKeys("password");
        driver.findElement(By.id("login")).click();

        // Generate and download report
        driver.findElement(By.id("generate-report")).click();
        driver.findElement(By.id("download-pdf")).click();

        // Download to local machine
        String filePath = downloader.downloadFile(driver, "monthly-report");

        // Verify
        File file = new File(filePath);
        assertTrue(file.exists(), "Report file should exist");
        assertTrue(file.length() > 1000, "Report should have content");

        System.out.println("✓ Report downloaded successfully: " + filePath);
    }

    @AfterClass
    public void teardown() {
        if (driver != null) {
            driver.quit();
        }
    }
}
```

## 6. Next Steps

- Read full [README.md](README.md) for advanced usage
- Check [ExampleUsage.java](src/main/java/com/selenium/downloader/ExampleUsage.java) for more examples
- Customize [download.properties](download.properties) for your environment

## Need Help?

1. Enable debug logging in your test
2. Check Selenium Grid logs
3. Verify network connectivity to Grid
4. Test SSH access manually (for SSH approach)
