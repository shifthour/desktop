# Selenium Remote File Downloader

A standalone Java utility to download files from remote Selenium Grid nodes to your local machine during test execution.

## Problem Statement

When running Selenium tests on a remote Selenium Grid, files downloaded during test execution are stored on the remote machine's download path. This utility provides multiple approaches to automatically transfer those files to your local machine.

## Features

- **Multiple Download Methods:**
  - Selenium 4 API (recommended for Grid 4+)
  - SSH/SCP file transfer
  - Chrome DevTools Protocol (CDP)

- **Configurable:**
  - Properties file support
  - Programmatic configuration
  - Customizable timeouts and paths

- **Production-Ready:**
  - Error handling and retry logic
  - Thread-safe operations
  - Comprehensive logging

## Prerequisites

- Java 11 or higher
- Maven 3.6+
- Selenium Grid 4+ (for Selenium 4 API approach)
- SSH access to Grid nodes (for SSH/SCP approach)

## Installation

### Clone/Copy the Project

```bash
cd selenium-remote-downloader
mvn clean install
```

### Add as Dependency (after publishing to local repo)

```xml
<dependency>
    <groupId>com.selenium</groupId>
    <artifactId>remote-file-downloader</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Quick Start

### Approach 1: Selenium 4 API (Recommended)

```java
import com.selenium.downloader.Selenium4Downloader;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.remote.RemoteWebDriver;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class QuickStart {
    public static void main(String[] args) throws Exception {
        // Setup Chrome options
        ChromeOptions options = new ChromeOptions();
        Map<String, Object> prefs = new HashMap<>();
        prefs.put("download.default_directory", "/tmp/downloads");
        prefs.put("download.prompt_for_download", false);
        options.setExperimentalOption("prefs", prefs);

        // Connect to remote Grid
        RemoteWebDriver driver = new RemoteWebDriver(
            new URL("http://localhost:4444/wd/hub"),
            options
        );

        try {
            // Create downloader
            Selenium4Downloader downloader = new Selenium4Downloader(
                "./downloads",  // Local path
                60              // Timeout in seconds
            );

            // Trigger download in your test
            driver.get("https://example.com/download-page");
            driver.findElement(By.id("download-btn")).click();

            // Wait and download file to local machine
            String localFile = downloader.downloadFile(driver, "report.pdf");
            System.out.println("Downloaded to: " + localFile);

        } finally {
            driver.quit();
        }
    }
}
```

### Approach 2: SSH/SCP Transfer

```java
import com.selenium.downloader.SSHFileTransfer;

public class SSHExample {
    public static void main(String[] args) throws Exception {
        // Configure SSH transfer
        SSHFileTransfer sshTransfer = new SSHFileTransfer.Builder()
            .remoteHost("192.168.1.100")           // Grid node IP
            .remotePort(22)
            .remoteUser("selenium")
            .remotePassword("password")             // Or use private key
            .remoteDownloadPath("/home/selenium/downloads")
            .localDownloadPath("./downloads")
            .timeoutSeconds(60)
            .build();

        // After triggering download in your test
        String localFile = sshTransfer.downloadFile("report.pdf");
        System.out.println("Transferred to: " + localFile);
    }
}
```

### Approach 3: Using Configuration File

```java
import com.selenium.downloader.DownloadConfig;

public class ConfigExample {
    public static void main(String[] args) throws Exception {
        // Load configuration from properties file
        DownloadConfig config = DownloadConfig.fromPropertiesFile("download.properties");

        // Use based on configured method
        switch (config.getDownloadMethod()) {
            case SELENIUM_4_API:
                Selenium4Downloader downloader = new Selenium4Downloader(
                    config.getLocalDownloadPath(),
                    config.getTimeoutSeconds()
                );
                // Use downloader...
                break;

            case SSH_SCP:
                SSHFileTransfer sshTransfer = new SSHFileTransfer.Builder()
                    .remoteHost(config.getSshHost())
                    .remoteUser(config.getSshUser())
                    .remotePassword(config.getSshPassword())
                    .remoteDownloadPath(config.getRemoteDownloadPath())
                    .localDownloadPath(config.getLocalDownloadPath())
                    .timeoutSeconds(config.getTimeoutSeconds())
                    .build();
                // Use SSH transfer...
                break;
        }
    }
}
```

## Configuration

### Properties File (download.properties)

```properties
# Local download path
local.download.path=./downloads

# Remote download path on Grid node
remote.download.path=/tmp/downloads

# Timeout in seconds
timeout.seconds=60

# Download method: SELENIUM_4_API, SSH_SCP, or CDP_PROTOCOL
download.method=SELENIUM_4_API

# SSH Configuration (for SSH_SCP method)
ssh.host=192.168.1.100
ssh.port=22
ssh.user=selenium
ssh.password=your_password
# OR use private key
# ssh.private.key.path=/path/to/private/key
```

## Detailed Usage Examples

### Example 1: Download PDF Report

```java
RemoteWebDriver driver = new RemoteWebDriver(
    new URL("http://grid:4444/wd/hub"),
    new ChromeOptions()
);

Selenium4Downloader downloader = new Selenium4Downloader("./reports", 120);

// Navigate and trigger download
driver.get("https://myapp.com/reports");
driver.findElement(By.id("generate-report")).click();

// Wait for file and download to local
String reportPath = downloader.downloadFile(driver, "monthly-report");
System.out.println("Report saved at: " + reportPath);

driver.quit();
```

### Example 2: Download with Custom Timeout

```java
// For large files, increase timeout
Selenium4Downloader downloader = new Selenium4Downloader(
    "/Users/myuser/Downloads",
    300  // 5 minutes timeout
);

String largeFile = downloader.downloadFile(driver, "large-dataset.zip");
```

### Example 3: Multiple Files Download

```java
Selenium4Downloader downloader = new Selenium4Downloader("./downloads", 60);

// Download multiple files
String[] expectedFiles = {"invoice.pdf", "receipt.pdf", "summary.xlsx"};

for (String fileName : expectedFiles) {
    // Trigger download in your test
    driver.findElement(By.linkText("Download " + fileName)).click();

    // Wait and download
    String localPath = downloader.downloadFile(driver, fileName);
    System.out.println("Downloaded: " + localPath);
}
```

### Example 4: SSH with Private Key

```java
SSHFileTransfer sshTransfer = new SSHFileTransfer.Builder()
    .remoteHost("grid-node.company.com")
    .remoteUser("griduser")
    .privateKeyPath("/home/user/.ssh/id_rsa")  // Use key instead of password
    .remoteDownloadPath("/opt/selenium/downloads")
    .localDownloadPath("./downloads")
    .timeoutSeconds(120)
    .build();

String file = sshTransfer.downloadFile("document.docx");
```

## How It Works

### Selenium 4 API Method

1. Selenium 4 Grid exposes downloadable files via REST API
2. The utility polls the `/session/{sessionId}/se/files` endpoint
3. When the expected file appears, it downloads the content
4. File is saved to your local specified path

### SSH/SCP Method

1. Connects to the remote Grid node via SSH
2. Polls the remote download directory for the file
3. Uses SCP (via JSch library) to transfer the file
4. Saves to local path

### CDP Method

1. Uses Chrome DevTools Protocol
2. Captures download events
3. Retrieves file content via CDP commands
4. Saves to local path

## Selenium Grid Setup

### For Selenium 4 API Method

Ensure your Grid 4 is properly configured:

```bash
# Start Grid Hub
java -jar selenium-server-4.x.x.jar hub

# Start Node with download support
java -jar selenium-server-4.x.x.jar node \
  --enable-managed-downloads true \
  --selenium-manager true
```

### For SSH/SCP Method

Ensure SSH access to Grid nodes:

```bash
# Test SSH connection
ssh selenium@grid-node-ip

# Verify download directory exists
ls -la /path/to/downloads
```

## Troubleshooting

### Issue: File not found within timeout

**Solution:**
- Increase timeout value
- Verify file is actually being downloaded on remote machine
- Check remote download path is correct
- Add wait for download to complete before calling download method

### Issue: SSH connection refused

**Solution:**
- Verify SSH service is running on Grid node
- Check firewall rules
- Verify credentials (username/password or private key)
- Test SSH connection manually

### Issue: Selenium 4 API returns empty file list

**Solution:**
- Ensure using Selenium Grid 4+
- Verify Grid node has `--enable-managed-downloads true` flag
- Check Grid node logs for errors
- Update to latest Selenium version

### Issue: Files download with wrong name

**Solution:**
- Use partial file name matching (e.g., "report" instead of "report-12345.pdf")
- Check the actual file name on remote machine
- Add logging to see what files are detected

## Advanced Usage

### Custom File Name Matching

```java
// Match files with pattern
public String downloadFileWithPattern(RemoteWebDriver driver, String pattern) {
    // Custom implementation
    List<String> files = getDownloadableFiles(driver);
    return files.stream()
        .filter(f -> f.matches(pattern))
        .findFirst()
        .orElseThrow();
}
```

### Parallel Downloads

```java
ExecutorService executor = Executors.newFixedThreadPool(3);

List<Future<String>> downloads = Arrays.asList(
    executor.submit(() -> downloader.downloadFile(driver1, "file1.pdf")),
    executor.submit(() -> downloader.downloadFile(driver2, "file2.pdf")),
    executor.submit(() -> downloader.downloadFile(driver3, "file3.pdf"))
);

for (Future<String> download : downloads) {
    System.out.println("Downloaded: " + download.get());
}

executor.shutdown();
```

## Project Structure

```
selenium-remote-downloader/
├── pom.xml
├── download.properties
├── README.md
└── src/main/java/com/selenium/downloader/
    ├── RemoteFileDownloader.java      # Main downloader with CDP
    ├── Selenium4Downloader.java       # Selenium 4 API implementation
    ├── SSHFileTransfer.java           # SSH/SCP implementation
    ├── DownloadConfig.java            # Configuration management
    └── ExampleUsage.java              # Usage examples
```

## Dependencies

- Selenium Java 4.27.0
- JSch 0.1.55 (for SSH)
- Apache Commons IO 2.15.1
- SLF4J 2.0.9

## Build and Run

```bash
# Build project
mvn clean package

# Run example
java -cp target/remote-file-downloader-1.0.0.jar \
  com.selenium.downloader.ExampleUsage

# Or run directly
mvn exec:java -Dexec.mainClass="com.selenium.downloader.ExampleUsage"
```

## Integration with TestNG/JUnit

```java
@Test
public void testFileDownload() throws Exception {
    // Setup
    Selenium4Downloader downloader = new Selenium4Downloader("./downloads", 60);

    // Execute test
    driver.get("https://app.com/download");
    driver.findElement(By.id("download")).click();

    // Download and verify
    String filePath = downloader.downloadFile(driver, "report.pdf");
    assertTrue(new File(filePath).exists());
    assertTrue(new File(filePath).length() > 0);
}
```

## Best Practices

1. **Use Selenium 4 API when possible** - It's the most reliable and doesn't require SSH access
2. **Set appropriate timeouts** - Large files need longer timeouts
3. **Clean up downloaded files** - Delete files after verification
4. **Use unique file names** - Avoid collisions in concurrent tests
5. **Handle exceptions** - Always wrap in try-catch and cleanup resources
6. **Log operations** - Enable logging to debug issues

## License

This is a standalone utility for educational and development purposes.

## Contributing

Feel free to enhance and customize based on your needs.

## Support

For issues and questions, please check:
- Selenium documentation: https://www.selenium.dev/documentation/
- Selenium Grid 4 docs: https://www.selenium.dev/documentation/grid/

## Version History

- **1.0.0** - Initial release with Selenium 4 API, SSH/SCP, and CDP support
