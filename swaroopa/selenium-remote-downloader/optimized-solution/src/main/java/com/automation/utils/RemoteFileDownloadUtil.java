package com.automation.utils;

import org.openqa.selenium.remote.RemoteWebDriver;
import org.openqa.selenium.remote.http.HttpClient;
import org.openqa.selenium.remote.http.HttpRequest;
import org.openqa.selenium.remote.http.HttpResponse;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;

/**
 * Optimized Utility Class for Remote File Download from Selenium Grid
 * Supports Selenium Grid 4+ with downloadable files API
 *
 * Usage:
 *   RemoteFileDownloadUtil util = new RemoteFileDownloadUtil(driver, "./downloads", 60);
 *   String localPath = util.downloadAndVerifyFile();
 */
public class RemoteFileDownloadUtil {

    private final RemoteWebDriver driver;
    private final String localDownloadPath;
    private final long timeoutSeconds;
    private final String sessionId;
    private final URL gridUrl;

    /**
     * Constructor
     *
     * @param driver RemoteWebDriver instance
     * @param localDownloadPath Local directory where files will be saved
     * @param timeoutSeconds Timeout in seconds to wait for file download
     */
    public RemoteFileDownloadUtil(RemoteWebDriver driver, String localDownloadPath, long timeoutSeconds) {
        this.driver = driver;
        this.localDownloadPath = localDownloadPath;
        this.timeoutSeconds = timeoutSeconds;
        this.sessionId = driver.getSessionId().toString();
        this.gridUrl = driver.getRemoteAddress();

        // Create local download directory if doesn't exist
        createLocalDirectory();

        System.out.println("RemoteFileDownloadUtil initialized:");
        System.out.println("  - Session ID: " + sessionId);
        System.out.println("  - Grid URL: " + gridUrl);
        System.out.println("  - Local Path: " + localDownloadPath);
        System.out.println("  - Timeout: " + timeoutSeconds + " seconds");
    }

    /**
     * Main method: Download and verify file from remote Grid
     * Auto-detects the latest downloaded file
     *
     * @return Local file path of the downloaded file
     * @throws Exception if download fails
     */
    public String downloadAndVerifyFile() throws Exception {
        return downloadAndVerifyFile(null);
    }

    /**
     * Download and verify specific file from remote Grid
     *
     * @param expectedFileName Expected file name (partial match allowed)
     * @return Local file path of the downloaded file
     * @throws Exception if download fails
     */
    public String downloadAndVerifyFile(String expectedFileName) throws Exception {
        System.out.println("Starting file download from remote Grid...");

        // Wait for file to appear in Grid's downloadable files
        String fileName = waitForFileInGrid(expectedFileName);

        // Download file content from Grid
        byte[] fileContent = downloadFileFromGrid(fileName);

        // Save to local path
        String localFilePath = saveToLocalPath(fileName, fileContent);

        System.out.println("✓ File successfully downloaded from remote Grid");
        return localFilePath;
    }

    /**
     * Wait for file to appear in Selenium Grid's downloadable files list
     */
    private String waitForFileInGrid(String expectedFileName) throws Exception {
        System.out.println("Waiting for file in remote Grid...");

        long startTime = System.currentTimeMillis();
        long maxWaitTime = timeoutSeconds * 1000;

        while (System.currentTimeMillis() - startTime < maxWaitTime) {
            try {
                List<String> availableFiles = getDownloadableFilesFromGrid();

                if (!availableFiles.isEmpty()) {
                    System.out.println("Available files in Grid: " + availableFiles);

                    // If specific file name provided, find it
                    if (expectedFileName != null && !expectedFileName.isEmpty()) {
                        Optional<String> matchedFile = availableFiles.stream()
                            .filter(file -> file.contains(expectedFileName))
                            .findFirst();

                        if (matchedFile.isPresent()) {
                            System.out.println("✓ Found matching file: " + matchedFile.get());
                            return matchedFile.get();
                        }
                    } else {
                        // Return the first/latest file
                        String file = availableFiles.get(availableFiles.size() - 1);
                        System.out.println("✓ Found file: " + file);
                        return file;
                    }
                }

            } catch (Exception e) {
                System.out.println("Polling for file... (" + e.getMessage() + ")");
            }

            Thread.sleep(1000); // Poll every second
        }

        throw new Exception("File not found in Grid within timeout of " + timeoutSeconds + " seconds" +
            (expectedFileName != null ? " for: " + expectedFileName : ""));
    }

    /**
     * Get list of downloadable files from Selenium Grid
     * Uses Selenium Grid 4 /session/{sessionId}/se/files endpoint
     */
    private List<String> getDownloadableFilesFromGrid() throws Exception {
        String listUrl = buildGridUrl("/session/" + sessionId + "/se/files");

        try {
            HttpClient client = HttpClient.Factory.createDefault().createClient(new URL(listUrl));
            HttpRequest request = new HttpRequest(org.openqa.selenium.remote.http.HttpMethod.GET, listUrl);
            HttpResponse response = client.execute(request);

            if (response.getStatus() == 200) {
                String responseBody = response.getContentString();
                return parseFileNames(responseBody);
            } else {
                throw new Exception("Failed to get file list. Status: " + response.getStatus());
            }

        } catch (Exception e) {
            throw new Exception("Error getting downloadable files from Grid: " + e.getMessage(), e);
        }
    }

    /**
     * Download file content from Selenium Grid
     * Uses /session/{sessionId}/se/files/{fileName} endpoint
     */
    private byte[] downloadFileFromGrid(String fileName) throws Exception {
        System.out.println("Downloading file from Grid: " + fileName);

        String downloadUrl = buildGridUrl("/session/" + sessionId + "/se/files/" + fileName);

        try {
            HttpClient client = HttpClient.Factory.createDefault().createClient(new URL(downloadUrl));
            HttpRequest request = new HttpRequest(org.openqa.selenium.remote.http.HttpMethod.GET, downloadUrl);
            HttpResponse response = client.execute(request);

            if (response.getStatus() == 200) {
                byte[] content = response.getContent().get();
                System.out.println("✓ Downloaded " + content.length + " bytes from Grid");
                return content;
            } else {
                throw new Exception("Failed to download file. Status: " + response.getStatus());
            }

        } catch (Exception e) {
            throw new Exception("Error downloading file from Grid: " + e.getMessage(), e);
        }
    }

    /**
     * Save file content to local path
     */
    private String saveToLocalPath(String fileName, byte[] content) throws IOException {
        Path localPath = Paths.get(localDownloadPath, fileName);

        Files.write(localPath, content);

        System.out.println("✓ File saved to local path: " + localPath.toAbsolutePath());
        return localPath.toAbsolutePath().toString();
    }

    /**
     * Build Grid URL with proper formatting
     */
    private String buildGridUrl(String endpoint) {
        String baseUrl = gridUrl.toString().replaceAll("/wd/hub$", "");
        return baseUrl + endpoint;
    }

    /**
     * Parse file names from Grid's JSON response
     * Simple parsing - for production use JSON library (Jackson/Gson)
     */
    private List<String> parseFileNames(String jsonResponse) {
        List<String> fileNames = new ArrayList<>();

        try {
            // Simple JSON parsing for "value": ["file1", "file2"]
            if (jsonResponse.contains("\"value\"")) {
                int startIndex = jsonResponse.indexOf("[", jsonResponse.indexOf("\"value\""));
                int endIndex = jsonResponse.indexOf("]", startIndex);

                if (startIndex != -1 && endIndex != -1) {
                    String arrayContent = jsonResponse.substring(startIndex + 1, endIndex);
                    String[] parts = arrayContent.split(",");

                    for (String part : parts) {
                        String fileName = part.trim()
                            .replace("\"", "")
                            .replace("'", "");

                        if (!fileName.isEmpty()) {
                            fileNames.add(fileName);
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error parsing file names: " + e.getMessage());
        }

        return fileNames;
    }

    /**
     * Create local download directory
     */
    private void createLocalDirectory() {
        try {
            Files.createDirectories(Paths.get(localDownloadPath));
            System.out.println("✓ Local download directory ready: " + localDownloadPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create local download directory: " + localDownloadPath, e);
        }
    }

    /**
     * Get the local download path
     */
    public String getLocalDownloadPath() {
        return localDownloadPath;
    }

    /**
     * Get timeout in seconds
     */
    public long getTimeoutSeconds() {
        return timeoutSeconds;
    }
}
