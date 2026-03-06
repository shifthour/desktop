package com.automation.utils;

import org.openqa.selenium.remote.RemoteWebDriver;
import org.openqa.selenium.remote.http.*;
import java.io.*;
import java.net.*;
import java.nio.file.*;

/**
 * Selenium 4.8 Compatible Version
 * Super Short Utility to Copy File from Remote Selenium Grid to Local Machine
 */
public class RemoteFileCopy_Selenium48 {

    private static RemoteWebDriver driver;

    public static void init(RemoteWebDriver d) {
        driver = d;
    }

    public static void copyLatestFile() throws Exception {
        String sessionId = driver.getSessionId().toString();
        String gridUrl = driver.getRemoteAddress().toString().replace("/wd/hub", "");
        String localPath = System.getProperty("user.home") + "/Downloads/";

        // Get files from Grid
        String listUrl = gridUrl + "/session/" + sessionId + "/se/files";

        // Selenium 4.8 compatible HTTP client creation
        HttpClient client = HttpClient.Factory.createDefault().createClient(new URL(gridUrl));
        HttpRequest request = new HttpRequest(HttpMethod.GET, listUrl);
        HttpResponse res = client.execute(request);

        if (res.getStatus() != 200) {
            throw new Exception("No files in Grid. Status: " + res.getStatus());
        }

        // Parse file name (simple)
        String body = res.getContentString();
        String fileName = body.substring(body.indexOf("[\"") + 2, body.indexOf("\"]"));

        // Download file
        String dlUrl = gridUrl + "/session/" + sessionId + "/se/files/" + fileName;
        HttpRequest dlRequest = new HttpRequest(HttpMethod.GET, dlUrl);
        HttpResponse dlRes = client.execute(dlRequest);

        if (dlRes.getStatus() != 200) {
            throw new Exception("Failed to download file. Status: " + dlRes.getStatus());
        }

        // Save to local - Selenium 4.8 compatible way
        byte[] content = dlRes.getContent().get();
        Files.write(Paths.get(localPath + fileName), content);

        System.out.println("✓ Copied: " + fileName + " to " + localPath);
    }

    public static void copyFile(String fileName) throws Exception {
        String sessionId = driver.getSessionId().toString();
        String gridUrl = driver.getRemoteAddress().toString().replace("/wd/hub", "");
        String localPath = System.getProperty("user.home") + "/Downloads/";

        // Download specific file
        String dlUrl = gridUrl + "/session/" + sessionId + "/se/files/" + fileName;

        HttpClient client = HttpClient.Factory.createDefault().createClient(new URL(gridUrl));
        HttpRequest dlRequest = new HttpRequest(HttpMethod.GET, dlUrl);
        HttpResponse dlRes = client.execute(dlRequest);

        if (dlRes.getStatus() != 200) {
            throw new Exception("File not found: " + fileName + ". Status: " + dlRes.getStatus());
        }

        // Save to local
        byte[] content = dlRes.getContent().get();
        Files.write(Paths.get(localPath + fileName), content);

        System.out.println("✓ Copied: " + fileName + " to " + localPath);
    }
}
