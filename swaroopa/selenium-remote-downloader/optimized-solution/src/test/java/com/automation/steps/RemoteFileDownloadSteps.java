package com.automation.steps;

import com.automation.utils.RemoteFileDownloadUtil;
import io.cucumber.java.en.Then;
import org.openqa.selenium.remote.RemoteWebDriver;
import org.testng.Assert;

import java.io.File;

/**
 * Optimized Step Definition for Remote File Download
 * Feature Line: "Then I verify the remote file download and copy to the local path"
 */
public class RemoteFileDownloadSteps {

    private RemoteWebDriver driver;
    private RemoteFileDownloadUtil fileDownloadUtil;

    public RemoteFileDownloadSteps() {
        // Get driver from your test context/hooks
        this.driver = getDriverFromContext();

        // Initialize file download utility
        String localPath = System.getProperty("local.download.path", "./downloads");
        long timeout = Long.parseLong(System.getProperty("download.timeout", "60"));

        this.fileDownloadUtil = new RemoteFileDownloadUtil(driver, localPath, timeout);
    }

    /**
     * Main step definition matching your feature line
     * Usage in feature file:
     *   Then I verify the remote file download and copy to the local path
     */
    @Then("I verify the remote file download and copy to the local path")
    public void iVerifyTheRemoteFileDownloadAndCopyToTheLocalPath() {
        try {
            System.out.println("Starting remote file download verification...");

            // Download file from remote Grid to local path
            String downloadedFilePath = fileDownloadUtil.downloadAndVerifyFile();

            // Verify file exists on local path
            File localFile = new File(downloadedFilePath);
            Assert.assertTrue(localFile.exists(),
                "File should exist at local path: " + downloadedFilePath);
            Assert.assertTrue(localFile.length() > 0,
                "Downloaded file should not be empty");

            System.out.println("✓ SUCCESS: File downloaded and verified");
            System.out.println("  - Local Path: " + downloadedFilePath);
            System.out.println("  - File Size: " + localFile.length() + " bytes");
            System.out.println("  - File Name: " + localFile.getName());

        } catch (Exception e) {
            System.err.println("✗ FAILED: Remote file download verification failed");
            System.err.println("  - Error: " + e.getMessage());
            Assert.fail("Remote file download and copy failed: " + e.getMessage(), e);
        }
    }

    /**
     * Overloaded step with specific filename
     * Usage: Then I verify the remote file download "report.pdf" and copy to the local path
     */
    @Then("I verify the remote file download {string} and copy to the local path")
    public void iVerifyTheRemoteFileDownloadWithNameAndCopyToTheLocalPath(String expectedFileName) {
        try {
            System.out.println("Verifying download for file: " + expectedFileName);

            // Download specific file
            String downloadedFilePath = fileDownloadUtil.downloadAndVerifyFile(expectedFileName);

            // Verify file
            File localFile = new File(downloadedFilePath);
            Assert.assertTrue(localFile.exists(),
                "File should exist: " + expectedFileName);
            Assert.assertTrue(localFile.getName().contains(expectedFileName),
                "File name should contain: " + expectedFileName);
            Assert.assertTrue(localFile.length() > 0,
                "File should not be empty");

            System.out.println("✓ SUCCESS: File '" + expectedFileName + "' downloaded and verified");
            System.out.println("  - Local Path: " + downloadedFilePath);
            System.out.println("  - File Size: " + localFile.length() + " bytes");

        } catch (Exception e) {
            System.err.println("✗ FAILED: Download verification for '" + expectedFileName + "' failed");
            Assert.fail("Remote file download failed for '" + expectedFileName + "': " + e.getMessage(), e);
        }
    }

    /**
     * Step with custom timeout
     * Usage: Then I verify the remote file download and copy to the local path with timeout 120 seconds
     */
    @Then("I verify the remote file download and copy to the local path with timeout {int} seconds")
    public void iVerifyTheRemoteFileDownloadWithTimeout(int timeoutSeconds) {
        try {
            System.out.println("Verifying download with timeout: " + timeoutSeconds + " seconds");

            // Create util with custom timeout
            RemoteFileDownloadUtil customUtil = new RemoteFileDownloadUtil(
                driver,
                System.getProperty("local.download.path", "./downloads"),
                timeoutSeconds
            );

            String downloadedFilePath = customUtil.downloadAndVerifyFile();

            // Verify file
            File localFile = new File(downloadedFilePath);
            Assert.assertTrue(localFile.exists(), "File should exist at: " + downloadedFilePath);
            Assert.assertTrue(localFile.length() > 0, "File should not be empty");

            System.out.println("✓ SUCCESS: File downloaded with custom timeout");
            System.out.println("  - Timeout Used: " + timeoutSeconds + " seconds");
            System.out.println("  - Local Path: " + downloadedFilePath);

        } catch (Exception e) {
            Assert.fail("Remote file download with timeout failed: " + e.getMessage(), e);
        }
    }

    /**
     * Helper method to get driver from Hooks
     */
    private RemoteWebDriver getDriverFromContext() {
        return com.automation.hooks.Hooks.getDriver();
    }
}
