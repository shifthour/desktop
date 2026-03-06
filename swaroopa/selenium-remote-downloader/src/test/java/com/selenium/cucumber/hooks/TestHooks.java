package com.selenium.cucumber.hooks;

import com.selenium.cucumber.utils.TestContext;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import io.cucumber.java.Scenario;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.remote.RemoteWebDriver;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Cucumber hooks for test setup and teardown
 */
public class TestHooks {

    private final TestContext context;

    public TestHooks() {
        this.context = TestContext.getInstance();
    }

    @Before(order = 0)
    public void beforeScenario(Scenario scenario) {
        System.out.println("========================================");
        System.out.println("Starting scenario: " + scenario.getName());
        System.out.println("========================================");

        // Clean up any previous test data
        context.cleanup();
    }

    @Before(order = 1)
    public void setupDriver(Scenario scenario) {
        try {
            // Get Grid URL from system property or use default
            String gridUrl = System.getProperty("grid.url", "http://localhost:4444/wd/hub");

            // Configure Chrome options
            ChromeOptions options = new ChromeOptions();

            // Set download directory
            String remoteDownloadPath = System.getProperty("remote.download.path", "/tmp/downloads");
            Map<String, Object> prefs = new HashMap<>();
            prefs.put("download.default_directory", remoteDownloadPath);
            prefs.put("download.prompt_for_download", false);
            prefs.put("download.directory_upgrade", true);
            prefs.put("safebrowsing.enabled", false);
            options.setExperimentalOption("prefs", prefs);

            // Add headless mode if specified
            if (Boolean.parseBoolean(System.getProperty("headless", "true"))) {
                options.addArguments("--headless");
                options.addArguments("--disable-gpu");
            }

            // Common Chrome arguments
            options.addArguments("--no-sandbox");
            options.addArguments("--disable-dev-shm-usage");
            options.addArguments("--disable-extensions");
            options.addArguments("--disable-infobars");
            options.addArguments("--window-size=1920,1080");

            // Create RemoteWebDriver
            RemoteWebDriver driver = new RemoteWebDriver(new URL(gridUrl), options);
            driver.manage().window().maximize();

            // Store in context
            context.setDriver(driver);

            System.out.println("✓ WebDriver initialized");
            System.out.println("  - Grid URL: " + gridUrl);
            System.out.println("  - Session ID: " + driver.getSessionId());
            System.out.println("  - Remote download path: " + remoteDownloadPath);

        } catch (Exception e) {
            System.err.println("Failed to initialize WebDriver: " + e.getMessage());
            throw new RuntimeException("WebDriver initialization failed", e);
        }
    }

    @Before(order = 2)
    public void setupDownloadDirectory() {
        try {
            // Create local download directory
            String localDownloadPath = System.getProperty("local.download.path", "./downloads");
            Files.createDirectories(Paths.get(localDownloadPath));
            System.out.println("✓ Local download directory ready: " + localDownloadPath);

        } catch (Exception e) {
            System.err.println("Failed to create download directory: " + e.getMessage());
        }
    }

    @After(order = 1)
    public void captureScreenshotOnFailure(Scenario scenario) {
        if (scenario.isFailed()) {
            try {
                RemoteWebDriver driver = context.getDriver();
                if (driver != null) {
                    byte[] screenshot = org.openqa.selenium.OutputType.BYTES.convertFromBase64Png(
                        driver.getScreenshotAs(org.openqa.selenium.OutputType.BASE64)
                    );
                    scenario.attach(screenshot, "image/png", "failure_screenshot");
                    System.out.println("✓ Screenshot captured for failed scenario");
                }
            } catch (Exception e) {
                System.err.println("Failed to capture screenshot: " + e.getMessage());
            }
        }
    }

    @After(order = 2)
    public void tearDownDriver(Scenario scenario) {
        try {
            RemoteWebDriver driver = context.getDriver();
            if (driver != null) {
                driver.quit();
                System.out.println("✓ WebDriver closed");
            }
        } catch (Exception e) {
            System.err.println("Error closing WebDriver: " + e.getMessage());
        }
    }

    @After(order = 3)
    public void afterScenario(Scenario scenario) {
        // Print scenario result
        System.out.println("----------------------------------------");
        System.out.println("Scenario: " + scenario.getName());
        System.out.println("Status: " + scenario.getStatus());

        // Print downloaded files if any
        if (!context.getDownloadedFiles().isEmpty()) {
            System.out.println("Downloaded files:");
            for (String file : context.getDownloadedFiles()) {
                File f = new File(file);
                System.out.println("  - " + f.getName() + " (" + f.length() + " bytes)");
            }
        }

        System.out.println("========================================\n");

        // Clean up context for next scenario
        TestContext.reset();
    }

    @After(order = 0, value = "@cleanup")
    public void cleanupDownloadedFiles(Scenario scenario) {
        try {
            // Optional: Clean up downloaded files after test
            for (String filePath : context.getDownloadedFiles()) {
                File file = new File(filePath);
                if (file.exists() && file.delete()) {
                    System.out.println("Deleted: " + file.getName());
                }
            }
        } catch (Exception e) {
            System.err.println("Error cleaning up files: " + e.getMessage());
        }
    }
}
