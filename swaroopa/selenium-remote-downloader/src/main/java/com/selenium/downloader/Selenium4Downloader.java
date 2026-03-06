package com.selenium.downloader;

import org.openqa.selenium.remote.RemoteWebDriver;
import org.openqa.selenium.remote.http.HttpClient;
import org.openqa.selenium.remote.http.HttpRequest;
import org.openqa.selenium.remote.http.HttpResponse;

import java.io.*;
import java.net.URL;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Selenium 4 specific implementation using downloadable files API
 * This works with Selenium Grid 4+ that supports the /session/{sessionId}/se/files endpoint
 */
public class Selenium4Downloader {

    private final String localDownloadPath;
    private final long timeoutSeconds;

    public Selenium4Downloader(String localDownloadPath, long timeoutSeconds) {
        this.localDownloadPath = localDownloadPath;
        this.timeoutSeconds = timeoutSeconds;

        try {
            Files.createDirectories(Paths.get(localDownloadPath));
        } catch (IOException e) {
            throw new RuntimeException("Failed to create local download directory", e);
        }
    }

    /**
     * Download file using Selenium 4's downloadable files endpoint
     * This is the recommended approach for Selenium Grid 4+
     */
    public String downloadFile(RemoteWebDriver driver, String expectedFileName)
            throws Exception {

        String sessionId = driver.getSessionId().toString();
        URL gridUrl = driver.getRemoteAddress();

        System.out.println("Grid URL: " + gridUrl);
        System.out.println("Session ID: " + sessionId);

        // Wait for file to be available
        String fileName = waitForFile(driver, expectedFileName);

        // Build the download URL
        String downloadUrl = String.format("%s/session/%s/se/files/%s",
            gridUrl.toString().replaceAll("/wd/hub$", ""),
            sessionId,
            fileName
        );

        System.out.println("Download URL: " + downloadUrl);

        // Download the file
        byte[] fileContent = downloadFileFromGrid(downloadUrl, driver);

        // Save to local path
        String localFilePath = Paths.get(localDownloadPath, fileName).toString();
        Files.write(Paths.get(localFilePath), fileContent);

        System.out.println("File downloaded successfully to: " + localFilePath);
        return localFilePath;
    }

    /**
     * Wait for file to appear in the downloadable files list
     */
    private String waitForFile(RemoteWebDriver driver, String expectedFileName)
            throws Exception {

        long startTime = System.currentTimeMillis();
        long maxWaitTime = timeoutSeconds * 1000;

        while (System.currentTimeMillis() - startTime < maxWaitTime) {
            try {
                List<String> files = getDownloadableFileNames(driver);
                System.out.println("Available files: " + files);

                Optional<String> matchingFile = files.stream()
                    .filter(file -> file.contains(expectedFileName))
                    .findFirst();

                if (matchingFile.isPresent()) {
                    return matchingFile.get();
                }

            } catch (Exception e) {
                System.out.println("Waiting for file... " + e.getMessage());
            }

            Thread.sleep(1000);
        }

        throw new Exception("File not found within timeout: " + expectedFileName);
    }

    /**
     * Get list of downloadable file names from Selenium Grid
     */
    private List<String> getDownloadableFileNames(RemoteWebDriver driver) throws Exception {
        String sessionId = driver.getSessionId().toString();
        URL gridUrl = driver.getRemoteAddress();

        String listUrl = String.format("%s/session/%s/se/files",
            gridUrl.toString().replaceAll("/wd/hub$", ""),
            sessionId
        );

        // Make HTTP GET request to get file list
        try {
            HttpClient client = HttpClient.Factory.createDefault().createClient(new URL(listUrl));
            HttpRequest request = new HttpRequest(org.openqa.selenium.remote.http.HttpMethod.GET, listUrl);
            HttpResponse response = client.execute(request);

            if (response.getStatus() == 200) {
                String responseBody = response.getContentString();
                // Parse JSON response to get file names
                return parseFileNamesFromResponse(responseBody);
            }

        } catch (Exception e) {
            System.err.println("Failed to get file list: " + e.getMessage());
        }

        return new ArrayList<>();
    }

    /**
     * Parse file names from JSON response
     */
    private List<String> parseFileNamesFromResponse(String jsonResponse) {
        List<String> fileNames = new ArrayList<>();

        try {
            // Simple JSON parsing - in production use a proper JSON library
            if (jsonResponse.contains("\"value\"")) {
                String value = jsonResponse.substring(
                    jsonResponse.indexOf("\"value\"") + 8
                );

                // Extract file names - this is simplified, use Jackson/Gson in production
                if (value.contains("[")) {
                    String[] parts = value.split("\"");
                    for (String part : parts) {
                        if (!part.trim().isEmpty() &&
                            !part.contains("[") &&
                            !part.contains("]") &&
                            !part.contains("{") &&
                            !part.contains("}") &&
                            !part.contains(":") &&
                            !part.equals(",") &&
                            !part.equals("value")) {
                            fileNames.add(part);
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
     * Download file content from Grid using HTTP client
     */
    private byte[] downloadFileFromGrid(String downloadUrl, RemoteWebDriver driver)
            throws Exception {

        try {
            HttpClient client = HttpClient.Factory.createDefault().createClient(new URL(downloadUrl));
            HttpRequest request = new HttpRequest(org.openqa.selenium.remote.http.HttpMethod.GET, downloadUrl);
            HttpResponse response = client.execute(request);

            if (response.getStatus() == 200) {
                return response.getContent().get();
            } else {
                throw new Exception("Failed to download file. Status: " + response.getStatus());
            }

        } catch (Exception e) {
            throw new Exception("Error downloading file from grid: " + e.getMessage(), e);
        }
    }
}
