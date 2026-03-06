package com.selenium.downloader;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.remote.RemoteWebDriver;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * Example usage of remote file downloader
 * Demonstrates three different approaches to download files from remote Selenium Grid
 */
public class ExampleUsage {

    public static void main(String[] args) {
        // Example 1: Using Selenium 4 API (Recommended for Selenium Grid 4+)
        exampleSelenium4Approach();

        // Example 2: Using SSH/SCP approach
        // exampleSSHApproach();

        // Example 3: Using CDP (Chrome DevTools Protocol)
        // exampleCDPApproach();
    }

    /**
     * Example 1: Selenium 4 API Approach
     * This is the recommended approach for Selenium Grid 4+
     */
    public static void exampleSelenium4Approach() {
        RemoteWebDriver driver = null;

        try {
            System.out.println("=== Selenium 4 API Approach ===");

            // Setup Chrome options with download preferences
            ChromeOptions options = new ChromeOptions();
            Map<String, Object> prefs = new HashMap<>();
            prefs.put("download.default_directory", "/tmp/downloads");
            prefs.put("download.prompt_for_download", false);
            prefs.put("download.directory_upgrade", true);
            prefs.put("safebrowsing.enabled", false);
            options.setExperimentalOption("prefs", prefs);

            // Enable downloads in headless mode
            options.addArguments("--headless");
            options.addArguments("--disable-gpu");
            options.addArguments("--no-sandbox");

            // Connect to remote Selenium Grid
            String gridUrl = "http://localhost:4444/wd/hub"; // Change to your Grid URL
            driver = new RemoteWebDriver(new URL(gridUrl), options);

            // Create downloader instance
            String localPath = "./downloads"; // Your local download path
            Selenium4Downloader downloader = new Selenium4Downloader(localPath, 60);

            // Navigate to a page and trigger download
            driver.get("https://file-examples.com/index.php/sample-documents-download/sample-pdf-download/");

            // Click download link (example)
            driver.findElement(By.linkText("Download sample pdf file")).click();

            // Wait for file to download and copy to local
            String expectedFileName = "file-sample"; // Partial file name
            String localFilePath = downloader.downloadFile(driver, expectedFileName);

            System.out.println("✓ File downloaded successfully: " + localFilePath);

        } catch (Exception e) {
            System.err.println("Error in Selenium 4 approach: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (driver != null) {
                driver.quit();
            }
        }
    }

    /**
     * Example 2: SSH/SCP Approach
     * Use this when Selenium Grid doesn't support file download API
     */
    public static void exampleSSHApproach() {
        RemoteWebDriver driver = null;

        try {
            System.out.println("=== SSH/SCP Approach ===");

            // Setup driver options
            ChromeOptions options = new ChromeOptions();
            Map<String, Object> prefs = new HashMap<>();
            prefs.put("download.default_directory", "/home/selenium/downloads"); // Remote download path
            options.setExperimentalOption("prefs", prefs);

            // Connect to remote Grid
            String gridUrl = "http://remote-grid:4444/wd/hub";
            driver = new RemoteWebDriver(new URL(gridUrl), options);

            // Configure SSH file transfer
            SSHFileTransfer sshTransfer = new SSHFileTransfer.Builder()
                .remoteHost("remote-grid-node.example.com") // Remote node hostname
                .remotePort(22)
                .remoteUser("selenium")
                .remotePassword("password") // Or use private key
                // .privateKeyPath("/path/to/private/key")
                .remoteDownloadPath("/home/selenium/downloads")
                .localDownloadPath("./downloads")
                .timeoutSeconds(60)
                .build();

            // Navigate and trigger download
            driver.get("https://example.com/download");
            driver.findElement(By.id("download-button")).click();

            // Copy file from remote to local via SSH
            String expectedFileName = "document.pdf";
            String localFilePath = sshTransfer.downloadFile(expectedFileName);

            System.out.println("✓ File transferred successfully: " + localFilePath);

        } catch (Exception e) {
            System.err.println("Error in SSH approach: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (driver != null) {
                driver.quit();
            }
        }
    }

    /**
     * Example 3: Chrome DevTools Protocol (CDP) Approach
     * Works with Chrome/Edge browsers
     */
    public static void exampleCDPApproach() {
        RemoteWebDriver driver = null;

        try {
            System.out.println("=== CDP Approach ===");

            ChromeOptions options = new ChromeOptions();

            // Connect to remote Grid
            String gridUrl = "http://localhost:4444/wd/hub";
            driver = new RemoteWebDriver(new URL(gridUrl), options);

            // Enable download via CDP
            Map<String, Object> params = new HashMap<>();
            params.put("behavior", "allow");
            params.put("downloadPath", "/tmp/downloads");
            driver.executeCdpCommand("Page.setDownloadBehavior", params);

            // Create downloader
            RemoteFileDownloader downloader = new RemoteFileDownloader("./downloads", 60);

            // Navigate and download
            driver.get("https://example.com/file.pdf");

            // Wait for download and copy to local
            String localFilePath = downloader.waitAndDownloadFile(driver, "file.pdf");

            System.out.println("✓ File downloaded via CDP: " + localFilePath);

            // Cleanup
            downloader.shutdown();

        } catch (Exception e) {
            System.err.println("Error in CDP approach: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (driver != null) {
                driver.quit();
            }
        }
    }

    /**
     * Example 4: Using configuration file
     */
    public static void exampleWithConfig() {
        RemoteWebDriver driver = null;

        try {
            // Load config from properties file
            DownloadConfig config = DownloadConfig.fromPropertiesFile("download.properties");
            System.out.println("Loaded config: " + config);

            // Setup driver
            ChromeOptions options = new ChromeOptions();
            driver = new RemoteWebDriver(new URL("http://localhost:4444/wd/hub"), options);

            // Use appropriate downloader based on config
            switch (config.getDownloadMethod()) {
                case SELENIUM_4_API:
                    Selenium4Downloader s4Downloader = new Selenium4Downloader(
                        config.getLocalDownloadPath(),
                        config.getTimeoutSeconds()
                    );
                    // Use downloader...
                    break;

                case SSH_SCP:
                    SSHFileTransfer sshTransfer = new SSHFileTransfer.Builder()
                        .remoteHost(config.getSshHost())
                        .remotePort(config.getSshPort())
                        .remoteUser(config.getSshUser())
                        .remotePassword(config.getSshPassword())
                        .remoteDownloadPath(config.getRemoteDownloadPath())
                        .localDownloadPath(config.getLocalDownloadPath())
                        .timeoutSeconds(config.getTimeoutSeconds())
                        .build();
                    // Use SSH transfer...
                    break;

                default:
                    throw new IllegalArgumentException("Unsupported download method");
            }

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (driver != null) {
                driver.quit();
            }
        }
    }
}
