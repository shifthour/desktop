package com.selenium.downloader;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.remote.RemoteWebDriver;
import org.openqa.selenium.remote.http.HttpClient;

import java.io.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/**
 * Main class to handle file downloads from remote Selenium Grid to local machine
 */
public class RemoteFileDownloader {

    private final String localDownloadPath;
    private final long timeoutSeconds;
    private final ExecutorService executorService;

    public RemoteFileDownloader(String localDownloadPath, long timeoutSeconds) {
        this.localDownloadPath = localDownloadPath;
        this.timeoutSeconds = timeoutSeconds;
        this.executorService = Executors.newSingleThreadExecutor();

        // Create local download directory if it doesn't exist
        try {
            Files.createDirectories(Paths.get(localDownloadPath));
        } catch (IOException e) {
            throw new RuntimeException("Failed to create local download directory: " + localDownloadPath, e);
        }
    }

    /**
     * Wait for a file to be downloaded and copy it to local path
     * This method polls for new files in the remote downloads directory
     *
     * @param driver The RemoteWebDriver instance
     * @param expectedFileName The expected file name (can be partial match)
     * @return Path to the downloaded file on local machine
     */
    public String waitAndDownloadFile(RemoteWebDriver driver, String expectedFileName)
            throws InterruptedException, ExecutionException, TimeoutException {

        Future<String> downloadFuture = executorService.submit(() -> {
            try {
                return downloadFileFromRemote(driver, expectedFileName);
            } catch (Exception e) {
                throw new RuntimeException("Failed to download file from remote", e);
            }
        });

        return downloadFuture.get(timeoutSeconds, TimeUnit.SECONDS);
    }

    /**
     * Internal method to handle the actual file download
     */
    private String downloadFileFromRemote(RemoteWebDriver driver, String expectedFileName)
            throws Exception {

        String sessionId = driver.getSessionId().toString();
        System.out.println("Session ID: " + sessionId);

        // Wait for file to appear in downloads
        long startTime = System.currentTimeMillis();
        long maxWaitTime = timeoutSeconds * 1000;

        while (System.currentTimeMillis() - startTime < maxWaitTime) {
            try {
                // Get list of downloadable files from remote driver
                List<String> downloadableFiles = getDownloadableFiles(driver);

                // Find the file matching expected name
                Optional<String> matchingFile = downloadableFiles.stream()
                    .filter(file -> file.contains(expectedFileName))
                    .findFirst();

                if (matchingFile.isPresent()) {
                    String fileName = matchingFile.get();
                    System.out.println("Found file: " + fileName);

                    // Download the file content
                    byte[] fileContent = downloadFileContent(driver, fileName);

                    // Save to local path
                    String localFilePath = Paths.get(localDownloadPath, fileName).toString();
                    Files.write(Paths.get(localFilePath), fileContent);

                    System.out.println("File downloaded successfully to: " + localFilePath);
                    return localFilePath;
                }

            } catch (Exception e) {
                System.out.println("Waiting for file... " + e.getMessage());
            }

            Thread.sleep(1000); // Poll every second
        }

        throw new TimeoutException("File not found within timeout period: " + expectedFileName);
    }

    /**
     * Get list of downloadable files from remote driver
     * This uses Selenium 4's downloadable files API
     */
    private List<String> getDownloadableFiles(RemoteWebDriver driver) throws Exception {
        try {
            // Selenium 4 approach - get downloadable file names
            @SuppressWarnings("unchecked")
            List<String> files = (List<String>) driver.executeScript(
                "return window.downloads || []"
            );

            if (files == null || files.isEmpty()) {
                // Alternative: Use CDP command to get download state
                Map<String, Object> result = driver.executeCdpCommand(
                    "Page.getDownloadableFiles",
                    new HashMap<>()
                );

                if (result.containsKey("files")) {
                    @SuppressWarnings("unchecked")
                    List<Map<String, String>> fileList = (List<Map<String, String>>) result.get("files");
                    return fileList.stream()
                        .map(file -> file.get("name"))
                        .filter(Objects::nonNull)
                        .toList();
                }
            }

            return files != null ? files : new ArrayList<>();

        } catch (Exception e) {
            System.err.println("Error getting downloadable files: " + e.getMessage());
            return new ArrayList<>();
        }
    }

    /**
     * Download file content from remote driver
     */
    private byte[] downloadFileContent(RemoteWebDriver driver, String fileName) throws Exception {
        try {
            // Try using CDP command to download file
            Map<String, Object> params = new HashMap<>();
            params.put("name", fileName);

            Map<String, Object> result = driver.executeCdpCommand(
                "Page.downloadFile",
                params
            );

            if (result.containsKey("content")) {
                String base64Content = (String) result.get("content");
                return Base64.getDecoder().decode(base64Content);
            }

        } catch (Exception e) {
            System.err.println("CDP download failed, trying alternative: " + e.getMessage());
        }

        throw new Exception("Unable to download file content from remote");
    }

    /**
     * Cleanup resources
     */
    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
