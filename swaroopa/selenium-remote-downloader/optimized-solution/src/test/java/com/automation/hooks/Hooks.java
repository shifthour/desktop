package com.automation.hooks;

import io.cucumber.java.After;
import io.cucumber.java.Before;
import io.cucumber.java.Scenario;
import org.openqa.selenium.OutputType;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.remote.RemoteWebDriver;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Cucumber Hooks for WebDriver Setup and Teardown
 */
public class Hooks {

    private static ThreadLocal<RemoteWebDriver> driver = new ThreadLocal<>();
    private static Properties config;

    static {
        // Load configuration
        config = loadConfig();
    }

    /**
     * Before hook - Initialize WebDriver
     */
    @Before(order = 0)
    public void setUp(Scenario scenario) {
        try {
            System.out.println("========================================");
            System.out.println("Starting Scenario: " + scenario.getName());
            System.out.println("========================================");

            // Get configuration
            String gridUrl = config.getProperty("grid.url", "http://localhost:4444/wd/hub");
            String remoteDownloadPath = config.getProperty("remote.download.path", "/tmp/downloads");
            String localDownloadPath = config.getProperty("local.download.path", "./downloads");
            boolean headless = Boolean.parseBoolean(config.getProperty("headless", "true"));

            // Configure Chrome options
            ChromeOptions options = new ChromeOptions();

            // Set download preferences for remote browser
            Map<String, Object> prefs = new HashMap<>();
            prefs.put("download.default_directory", remoteDownloadPath);
            prefs.put("download.prompt_for_download", false);
            prefs.put("download.directory_upgrade", true);
            prefs.put("safebrowsing.enabled", false);
            options.setExperimentalOption("prefs", prefs);

            // Add browser arguments
            if (headless) {
                options.addArguments("--headless");
                options.addArguments("--disable-gpu");
            }
            options.addArguments("--no-sandbox");
            options.addArguments("--disable-dev-shm-usage");
            options.addArguments("--window-size=1920,1080");

            // Create RemoteWebDriver
            RemoteWebDriver remoteDriver = new RemoteWebDriver(new URL(gridUrl), options);
            driver.set(remoteDriver);

            // Create local download directory
            Files.createDirectories(Paths.get(localDownloadPath));

            System.out.println("✓ WebDriver initialized successfully");
            System.out.println("  - Grid URL: " + gridUrl);
            System.out.println("  - Session ID: " + remoteDriver.getSessionId());
            System.out.println("  - Remote Download Path: " + remoteDownloadPath);
            System.out.println("  - Local Download Path: " + localDownloadPath);
            System.out.println("  - Headless: " + headless);

        } catch (Exception e) {
            System.err.println("Failed to initialize WebDriver: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("WebDriver initialization failed", e);
        }
    }

    /**
     * After hook - Capture screenshot on failure
     */
    @After(order = 1)
    public void captureScreenshot(Scenario scenario) {
        if (scenario.isFailed()) {
            try {
                RemoteWebDriver webDriver = driver.get();
                if (webDriver != null) {
                    byte[] screenshot = webDriver.getScreenshotAs(OutputType.BYTES);
                    scenario.attach(screenshot, "image/png", "failure-screenshot");
                    System.out.println("✓ Screenshot captured for failed scenario");
                }
            } catch (Exception e) {
                System.err.println("Failed to capture screenshot: " + e.getMessage());
            }
        }
    }

    /**
     * After hook - Close WebDriver
     */
    @After(order = 0)
    public void tearDown(Scenario scenario) {
        try {
            RemoteWebDriver webDriver = driver.get();
            if (webDriver != null) {
                webDriver.quit();
                System.out.println("✓ WebDriver closed");
            }
        } catch (Exception e) {
            System.err.println("Error closing WebDriver: " + e.getMessage());
        } finally {
            driver.remove();
        }

        System.out.println("========================================");
        System.out.println("Scenario " + scenario.getName() + " - " + scenario.getStatus());
        System.out.println("========================================\n");
    }

    /**
     * Get the current driver instance (used by step definitions)
     */
    public static RemoteWebDriver getDriver() {
        return driver.get();
    }

    /**
     * Load configuration from properties file
     */
    private static Properties loadConfig() {
        Properties props = new Properties();
        try {
            String configPath = System.getProperty("config.file", "config.properties");
            if (Files.exists(Paths.get(configPath))) {
                props.load(new FileInputStream(configPath));
                System.out.println("✓ Configuration loaded from: " + configPath);
            } else {
                System.out.println("Using default configuration (config.properties not found)");
            }
        } catch (IOException e) {
            System.err.println("Failed to load config: " + e.getMessage());
        }

        // Override with system properties if provided
        System.getProperties().forEach((key, value) -> {
            if (key.toString().startsWith("grid.") ||
                key.toString().startsWith("local.") ||
                key.toString().startsWith("remote.") ||
                key.toString().startsWith("download.")) {
                props.setProperty(key.toString(), value.toString());
            }
        });

        return props;
    }
}
