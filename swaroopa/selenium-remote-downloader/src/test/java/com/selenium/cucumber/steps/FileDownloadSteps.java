package com.selenium.cucumber.steps;

import com.selenium.cucumber.utils.TestContext;
import com.selenium.downloader.DownloadConfig;
import com.selenium.downloader.Selenium4Downloader;
import com.selenium.downloader.SSHFileTransfer;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.openqa.selenium.By;
import org.openqa.selenium.remote.RemoteWebDriver;
import org.testng.Assert;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Step definitions for file download verification
 */
public class FileDownloadSteps {

    private final TestContext context;
    private final RemoteWebDriver driver;

    public FileDownloadSteps() {
        this.context = TestContext.getInstance();
        this.driver = context.getDriver();
    }

    // ==================== GIVEN Steps ====================

    @Given("the remote file download is configured")
    public void theRemoteFileDownloadIsConfigured() {
        try {
            // Load configuration from properties file or use default
            DownloadConfig config;
            String configPath = System.getProperty("download.config", "download.properties");

            if (Files.exists(Paths.get(configPath))) {
                config = DownloadConfig.fromPropertiesFile(configPath);
                System.out.println("Loaded config from: " + configPath);
            } else {
                // Use default configuration
                config = new DownloadConfig.Builder()
                    .localDownloadPath(System.getProperty("local.download.path", "./downloads"))
                    .downloadMethod(DownloadConfig.DownloadMethod.SELENIUM_4_API)
                    .timeoutSeconds(Long.parseLong(System.getProperty("download.timeout", "60")))
                    .build();
                System.out.println("Using default configuration");
            }

            context.setConfig(config);

            // Initialize appropriate downloader based on config
            switch (config.getDownloadMethod()) {
                case SELENIUM_4_API:
                    Selenium4Downloader s4Downloader = new Selenium4Downloader(
                        config.getLocalDownloadPath(),
                        config.getTimeoutSeconds()
                    );
                    context.setSelenium4Downloader(s4Downloader);
                    System.out.println("✓ Selenium 4 downloader initialized");
                    break;

                case SSH_SCP:
                    SSHFileTransfer sshTransfer = new SSHFileTransfer.Builder()
                        .remoteHost(config.getSshHost())
                        .remotePort(config.getSshPort())
                        .remoteUser(config.getSshUser())
                        .remotePassword(config.getSshPassword())
                        .privateKeyPath(config.getSshPrivateKeyPath())
                        .remoteDownloadPath(config.getRemoteDownloadPath())
                        .localDownloadPath(config.getLocalDownloadPath())
                        .timeoutSeconds(config.getTimeoutSeconds())
                        .build();
                    context.setSshFileTransfer(sshTransfer);
                    System.out.println("✓ SSH file transfer initialized");
                    break;

                default:
                    throw new IllegalStateException("Unsupported download method: " + config.getDownloadMethod());
            }

        } catch (Exception e) {
            Assert.fail("Failed to configure remote file download: " + e.getMessage(), e);
        }
    }

    @Given("I am on the download page")
    public void iAmOnTheDownloadPage() {
        String downloadUrl = System.getProperty("download.page.url", "https://example.com/download");
        driver.get(downloadUrl);
        System.out.println("Navigated to download page: " + downloadUrl);
    }

    @Given("I am on the reports page")
    public void iAmOnTheReportsPage() {
        String reportsUrl = System.getProperty("reports.page.url", "https://example.com/reports");
        driver.get(reportsUrl);
        System.out.println("Navigated to reports page: " + reportsUrl);
    }

    @Given("I am on the documents page")
    public void iAmOnTheDocumentsPage() {
        String documentsUrl = System.getProperty("documents.page.url", "https://example.com/documents");
        driver.get(documentsUrl);
        System.out.println("Navigated to documents page: " + documentsUrl);
    }

    @Given("I am on the export page")
    public void iAmOnTheExportPage() {
        String exportUrl = System.getProperty("export.page.url", "https://example.com/export");
        driver.get(exportUrl);
        System.out.println("Navigated to export page: " + exportUrl);
    }

    // ==================== WHEN Steps ====================

    @When("I click on download button")
    public void iClickOnDownloadButton() {
        try {
            driver.findElement(By.id("download-button")).click();
            System.out.println("Clicked download button");
            Thread.sleep(1000); // Wait for download to initiate
        } catch (Exception e) {
            context.setLastException(e);
            System.err.println("Failed to click download button: " + e.getMessage());
        }
    }

    @When("I generate monthly report")
    public void iGenerateMonthlyReport() {
        try {
            driver.findElement(By.id("generate-report")).click();
            System.out.println("Clicked generate report button");
            Thread.sleep(2000); // Wait for report generation
        } catch (Exception e) {
            context.setLastException(e);
        }
    }

    @When("I click on download PDF button")
    public void iClickOnDownloadPDFButton() {
        try {
            driver.findElement(By.id("download-pdf")).click();
            System.out.println("Clicked download PDF button");
            Thread.sleep(1000);
        } catch (Exception e) {
            context.setLastException(e);
        }
    }

    @When("I download the following files:")
    public void iDownloadTheFollowingFiles(List<String> fileNames) {
        for (String fileName : fileNames) {
            try {
                String sanitizedName = fileName.replace(".", "-");
                driver.findElement(By.id("download-" + sanitizedName)).click();
                System.out.println("Triggered download for: " + fileName);
                Thread.sleep(500);
            } catch (Exception e) {
                context.setLastException(e);
                System.err.println("Failed to download: " + fileName);
            }
        }
    }

    @When("I select {string} export option")
    public void iSelectExportOption(String format) {
        try {
            driver.findElement(By.id("export-format")).sendKeys(format);
            System.out.println("Selected export format: " + format);
        } catch (Exception e) {
            context.setLastException(e);
        }
    }

    @When("I click on export button")
    public void iClickOnExportButton() {
        try {
            driver.findElement(By.id("export-button")).click();
            System.out.println("Clicked export button");
            Thread.sleep(1000);
        } catch (Exception e) {
            context.setLastException(e);
        }
    }

    @When("I download large dataset file")
    public void iDownloadLargeDatasetFile() {
        try {
            driver.findElement(By.id("download-large-file")).click();
            System.out.println("Triggered large file download");
        } catch (Exception e) {
            context.setLastException(e);
        }
    }

    @When("I trigger invalid download")
    public void iTriggerInvalidDownload() {
        try {
            driver.findElement(By.id("invalid-download")).click();
        } catch (Exception e) {
            context.setLastException(e);
        }
    }

    // ==================== THEN Steps (Main Verification) ====================

    @Then("verify file download on the remote machine")
    public void verifyFileDownloadOnTheRemoteMachine() {
        try {
            String downloadedFile = downloadFileFromRemote("download");

            // Verify file exists
            File file = new File(downloadedFile);
            Assert.assertTrue(file.exists(), "Downloaded file should exist: " + downloadedFile);
            Assert.assertTrue(file.length() > 0, "Downloaded file should not be empty");

            System.out.println("✓ File verified successfully: " + downloadedFile);
            System.out.println("  - Size: " + file.length() + " bytes");

        } catch (Exception e) {
            Assert.fail("File download verification failed: " + e.getMessage(), e);
        }
    }

    @Then("verify file download on the remote machine with filename {string}")
    public void verifyFileDownloadOnTheRemoteMachineWithFilename(String expectedFileName) {
        try {
            String downloadedFile = downloadFileFromRemote(expectedFileName);

            // Verify file exists and name matches
            File file = new File(downloadedFile);
            Assert.assertTrue(file.exists(), "File should exist: " + expectedFileName);
            Assert.assertTrue(file.getName().contains(expectedFileName),
                "File name should contain: " + expectedFileName);
            Assert.assertTrue(file.length() > 0, "File should not be empty");

            System.out.println("✓ File verified: " + downloadedFile);
            System.out.println("  - Expected: " + expectedFileName);
            System.out.println("  - Actual: " + file.getName());
            System.out.println("  - Size: " + file.length() + " bytes");

        } catch (Exception e) {
            Assert.fail("File download verification failed for '" + expectedFileName + "': " + e.getMessage(), e);
        }
    }

    @Then("verify all files are downloaded on the remote machine")
    public void verifyAllFilesAreDownloadedOnTheRemoteMachine() {
        List<String> downloadedFiles = context.getDownloadedFiles();

        Assert.assertFalse(downloadedFiles.isEmpty(), "At least one file should be downloaded");

        for (String filePath : downloadedFiles) {
            File file = new File(filePath);
            Assert.assertTrue(file.exists(), "File should exist: " + filePath);
            Assert.assertTrue(file.length() > 0, "File should not be empty: " + filePath);
            System.out.println("✓ Verified: " + file.getName() + " (" + file.length() + " bytes)");
        }

        System.out.println("✓ All " + downloadedFiles.size() + " files verified successfully");
    }

    @Then("verify file download on the remote machine with extension {string}")
    public void verifyFileDownloadOnTheRemoteMachineWithExtension(String extension) {
        try {
            String downloadedFile = downloadFileFromRemote("export");

            File file = new File(downloadedFile);
            Assert.assertTrue(file.exists(), "File should exist");
            Assert.assertTrue(file.getName().endsWith(extension),
                "File should have extension: " + extension);
            Assert.assertTrue(file.length() > 0, "File should not be empty");

            System.out.println("✓ File verified with extension " + extension + ": " + downloadedFile);

        } catch (Exception e) {
            Assert.fail("File verification failed for extension '" + extension + "': " + e.getMessage(), e);
        }
    }

    @Then("verify file download on the remote machine with timeout {int} seconds")
    public void verifyFileDownloadOnTheRemoteMachineWithTimeout(int timeoutSeconds) {
        try {
            context.setCustomTimeout(timeoutSeconds);
            String downloadedFile = downloadFileFromRemote("large-file");

            File file = new File(downloadedFile);
            Assert.assertTrue(file.exists(), "Large file should be downloaded");
            Assert.assertTrue(file.length() > 0, "Large file should not be empty");

            System.out.println("✓ Large file downloaded successfully with " + timeoutSeconds + "s timeout");
            System.out.println("  - File: " + downloadedFile);
            System.out.println("  - Size: " + file.length() + " bytes");

        } catch (Exception e) {
            Assert.fail("Large file download failed: " + e.getMessage(), e);
        } finally {
            context.setCustomTimeout(60); // Reset to default
        }
    }

    @Then("verify download fails gracefully with error message")
    public void verifyDownloadFailsGracefullyWithErrorMessage() {
        Exception lastException = context.getLastException();
        Assert.assertNotNull(lastException, "An exception should have been caught");
        System.out.println("✓ Download failed gracefully with error: " + lastException.getMessage());
    }

    // ==================== Helper Methods ====================

    /**
     * Core method to download file from remote machine
     * Uses the configured download method (Selenium 4 API or SSH/SCP)
     */
    private String downloadFileFromRemote(String expectedFileName) throws Exception {
        DownloadConfig config = context.getConfig();
        String localFilePath;

        switch (config.getDownloadMethod()) {
            case SELENIUM_4_API:
                Selenium4Downloader s4Downloader = context.getSelenium4Downloader();

                // Create new downloader with custom timeout if set
                if (context.getCustomTimeout() != 60) {
                    s4Downloader = new Selenium4Downloader(
                        config.getLocalDownloadPath(),
                        context.getCustomTimeout()
                    );
                }

                System.out.println("Downloading file using Selenium 4 API...");
                localFilePath = s4Downloader.downloadFile(driver, expectedFileName);
                break;

            case SSH_SCP:
                SSHFileTransfer sshTransfer = context.getSshFileTransfer();
                System.out.println("Downloading file using SSH/SCP...");
                localFilePath = sshTransfer.downloadFile(expectedFileName);
                break;

            default:
                throw new IllegalStateException("Unsupported download method: " + config.getDownloadMethod());
        }

        // Store in context
        context.setLastDownloadedFile(localFilePath);

        return localFilePath;
    }
}
