package com.selenium.cucumber.utils;

import com.selenium.downloader.DownloadConfig;
import com.selenium.downloader.Selenium4Downloader;
import com.selenium.downloader.SSHFileTransfer;
import org.openqa.selenium.remote.RemoteWebDriver;

import java.util.ArrayList;
import java.util.List;

/**
 * Test Context to share state between Cucumber step definitions
 * Thread-safe using ThreadLocal for parallel execution
 */
public class TestContext {

    private static final ThreadLocal<TestContext> instance = ThreadLocal.withInitial(TestContext::new);

    private RemoteWebDriver driver;
    private Selenium4Downloader selenium4Downloader;
    private SSHFileTransfer sshFileTransfer;
    private DownloadConfig config;
    private String lastDownloadedFile;
    private List<String> downloadedFiles;
    private Exception lastException;
    private long customTimeout;

    private TestContext() {
        this.downloadedFiles = new ArrayList<>();
        this.customTimeout = 60; // Default timeout
    }

    public static TestContext getInstance() {
        return instance.get();
    }

    public static void reset() {
        instance.remove();
    }

    // Driver management
    public RemoteWebDriver getDriver() {
        return driver;
    }

    public void setDriver(RemoteWebDriver driver) {
        this.driver = driver;
    }

    // Downloader management
    public Selenium4Downloader getSelenium4Downloader() {
        return selenium4Downloader;
    }

    public void setSelenium4Downloader(Selenium4Downloader downloader) {
        this.selenium4Downloader = downloader;
    }

    public SSHFileTransfer getSshFileTransfer() {
        return sshFileTransfer;
    }

    public void setSshFileTransfer(SSHFileTransfer transfer) {
        this.sshFileTransfer = transfer;
    }

    // Configuration
    public DownloadConfig getConfig() {
        return config;
    }

    public void setConfig(DownloadConfig config) {
        this.config = config;
    }

    // Downloaded files tracking
    public String getLastDownloadedFile() {
        return lastDownloadedFile;
    }

    public void setLastDownloadedFile(String filePath) {
        this.lastDownloadedFile = filePath;
        this.downloadedFiles.add(filePath);
    }

    public List<String> getDownloadedFiles() {
        return new ArrayList<>(downloadedFiles);
    }

    public void clearDownloadedFiles() {
        this.downloadedFiles.clear();
        this.lastDownloadedFile = null;
    }

    // Exception handling
    public Exception getLastException() {
        return lastException;
    }

    public void setLastException(Exception exception) {
        this.lastException = exception;
    }

    // Custom timeout
    public long getCustomTimeout() {
        return customTimeout;
    }

    public void setCustomTimeout(long timeout) {
        this.customTimeout = timeout;
    }

    // Cleanup
    public void cleanup() {
        this.lastDownloadedFile = null;
        this.downloadedFiles.clear();
        this.lastException = null;
        this.customTimeout = 60;
    }
}
