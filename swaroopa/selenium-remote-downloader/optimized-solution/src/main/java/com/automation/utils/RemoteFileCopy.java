package com.automation.utils;

import org.openqa.selenium.remote.RemoteWebDriver;
import org.openqa.selenium.remote.http.*;
import java.net.URL;
import java.nio.file.*;
import java.util.*;

/**
 * Super Short Utility to Copy File from Remote Selenium Grid to Local Machine
 */
public class RemoteFileCopy {

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
        HttpClient client = HttpClient.Factory.createDefault().createClient(new URL(listUrl));
        HttpResponse res = client.execute(new HttpRequest(HttpMethod.GET, listUrl));

        if (res.getStatus() != 200) throw new Exception("No files in Grid");

        // Parse file name (simple)
        String body = res.getContentString();
        String fileName = body.substring(body.indexOf("[\"") + 2, body.indexOf("\"]"));

        // Download file
        String dlUrl = gridUrl + "/session/" + sessionId + "/se/files/" + fileName;
        HttpResponse dlRes = client.execute(new HttpRequest(HttpMethod.GET, dlUrl));

        // Save to local
        Files.write(Paths.get(localPath + fileName), dlRes.getContent().get());

        System.out.println("✓ Copied: " + fileName + " to " + localPath);
    }

    public static void copyFile(String fileName) throws Exception {
        String sessionId = driver.getSessionId().toString();
        String gridUrl = driver.getRemoteAddress().toString().replace("/wd/hub", "");
        String localPath = System.getProperty("user.home") + "/Downloads/";

        // Download specific file
        String dlUrl = gridUrl + "/session/" + sessionId + "/se/files/" + fileName;
        HttpClient client = HttpClient.Factory.createDefault().createClient(new URL(dlUrl));
        HttpResponse dlRes = client.execute(new HttpRequest(HttpMethod.GET, dlUrl));

        if (dlRes.getStatus() != 200) throw new Exception("File not found: " + fileName);

        // Save to local
        Files.write(Paths.get(localPath + fileName), dlRes.getContent().get());

        System.out.println("✓ Copied: " + fileName + " to " + localPath);
    }
}
