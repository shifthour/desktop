package com.cao.pageObjects.FCTLocator;

import baseFiles.BaseConfig;
import commonUtilities.Logs;
import io.restassured.response.Response;
import io.restassured.RestAssured;
import org.json.JSONObject;
import org.openqa.selenium.Cookie;
import org.openqa.selenium.WebDriver;
import org.json.JSONArray;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.openqa.selenium.devtools.DevTools;
import org.openqa.selenium.devtools.HasDevTools;
import org.openqa.selenium.devtools.v139.network.Network;
import org.openqa.selenium.devtools.v139.network.model.Request;
import java.util.concurrent.CopyOnWriteArrayList;

public class StatusRevert {
    private static DevTools devTools;
    private static final List<String> capturedPayloads = new CopyOnWriteArrayList<>();
    private static final Properties properties = new Properties();
    private static final String PROPERTIES_FILE =
        "src/test/resources/config/apiconfig.properties";
    private static final Map<String, String> mappedData = new HashMap<>();
    private static final Map<String, String> response = new HashMap<>();
    private static final Map<String, String> initialValues = new HashMap<>();
    private static final Map<String, String> updatedValues = new HashMap<>();
    private static WebDriver driver;
    private static String sessionCookie;
    private static List<String> fcid;

    public StatusRevert() throws InterruptedException {
        driver = BrowserConfig.getDriver();
        this.fcid = new ArrayList<>();
        Logs.info("StatusRevert initialized with driver: " + driver);
    }

    static {
        try {
            FileInputStream inputStream = new FileInputStream(PROPERTIES_FILE);
            properties.load(inputStream);
            Logs.info("Properties file loaded successfully: " +
                properties.getProperty("base.url"));
        } catch (IOException e) {
            Logs.info("Failed to load properties file: " + PROPERTIES_FILE);
            throw new RuntimeException("Failed to load properties file: " +
                PROPERTIES_FILE, e);
        }
    }

    public static void enableNetworkCapture(WebDriver driver)
        throws InterruptedException, IOException {
        try {
            // Check if DevTools is available for this browser
            if (driver instanceof HasDevTools) {
                devTools = ((HasDevTools) driver).getDevTools();
                devTools.createSession();
                devTools.send(Network.enable(Optional.empty(), Optional.empty(),
                    Optional.empty()));
                Logs.info("Network capture enabled successfully.");
                devTools.addListener(Network.requestWillBeSent(), event -> {
                    Request req = event.getRequest();
                    String url = req.getUrl();
                    if (url.contains("bulkEditAdmin") && req.getPostData().isPresent()) {
                        List<String> fcids = extractFCIds(req.getPostData().get());
                        capturedPayloads.addAll(fcids);
                        Logs.info("Captured FC id's: " + fcids);
                    }
                    Logs.info("Request URL: " + url);
                    Logs.info("Request Method: " + req.getMethod());
                });
                devTools.addListener(Network.responseReceived(), event -> {
                    Response res = (Response) event.getResponse();
                    // Logs.info("Response URL: " + res.getUrl());
                });
                devTools.addListener(Network.loadingFailed(), event -> {
                    Logs.info("Loading failed for URL: " + event.getRequestId() + " " +
                        "with error: " + event.getErrorText());
                });
            } else {
                Logs.info("DevTools not supported for this browser. Network capture skipped.");
            }
        } catch (Exception e) {
            Logs.error("Failed to enable network capture: " + e.getMessage());
            throw new RuntimeException("Failed to enable network capture", e);
        }
    }

    public static void disableNetworkCapture(WebDriver driver) {
        if (devTools != null) {
            try {
                devTools.send(Network.disable());
                Logs.info("Network capture disabled successfully.");
            } catch (Exception e) {
                Logs.error("Failed to disable network capture: " + e.getMessage());
            }
        } else {
            Logs.error("DevTools is not initialized. Cannot disable network capture.");
        }
    }

    private static List<String> extractFCIds(String postData) {
        List<String> fcids = new ArrayList<>();
        var matcher =
            java.util.regex.Pattern.compile("\"fcId\":\"(.+?)\"").matcher(postData);
        while (matcher.find()) {
            fcids.add(matcher.group(1));
            Logs.info("Extracted FCID: " + matcher.group(1));
        }
        return fcids;
    }

    public static void setFeatureValues(Map<String, String> initialValues,
        Map<String, String> updatedValues) {
        initialValues.clear();
        updatedValues.clear();
        StatusRevert.initialValues.putAll(initialValues);
        StatusRevert.updatedValues.putAll(updatedValues);
        Logs.info("Feature values set successfully.");
        mapValuesToDB(initialValues);
        mapValuesToDB(updatedValues);
    }

    private static void mapValuesToDB(Map<String, String> values) {
        values.replaceAll((k, v) -> {
            if (k.equalsIgnoreCase("status") || k.equalsIgnoreCase("comments") ||
                k.equalsIgnoreCase("reason")) {
                String dbKey = properties.getProperty(k + ".key");
                return properties.getProperty(v, v);
            }
            return v;
        });
    }

    private static String buildRevertPayload(String fcid) {
        StringBuilder json = new StringBuilder("{");
        json.append("\"fcId\":\"").append(fcid).append("\",");
        for (String field : List.of("status", "comments", "reason", "date")) {
            String initialValue = initialValues.get(field);
            String updatedValue = updatedValues.get(field);
            if (updatedValue != null && !updatedValue.equalsIgnoreCase(initialValue)) {
                json.append(",");
                json.append("\"").append(field).append("\":").append("{\"from\":\"")
                    .append(updatedValue).append("\",\"to\":\"")
                    .append(initialValue).append("\"}");
                Logs.info("Added " + field + ": " + updatedValue + " to JSON object.");
            }
        }
        json.append("}");
        return json.toString();
    }

    public static void revertEditedRecords() {
        if (capturedPayloads.isEmpty()) {
            Logs.info("No captured payloads to revert.");
            return;
        }
        String baseUrl = properties.getProperty("base.url");
        String queryEndpoint = properties.getProperty("query.endpoint");
        for (String fcid : capturedPayloads) {
            String payload = buildRevertPayload(fcid);
            Logs.info("Reverting FCID: " + fcid + " with payload: " + payload);
            int responseCode = callRevertAPI(baseUrl + queryEndpoint, payload);
            response.put(fcid, String.valueOf(responseCode));
            Logs.info("Response code for FCID " + fcid + ": " + responseCode);
        }
    }

    public static int callRevertAPI(String apiUrl, String payload) {
        try {
            URL url = new URL(apiUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setDoOutput(true);

            connection.getOutputStream().write(payload.getBytes(StandardCharsets.UTF_8));

            int code = connection.getResponseCode();
            Logs.info("payload: " + payload + " response code: " + code);
            connection.disconnect();
            return code;
        } catch (IOException e) {
            Logs.error("Failed to call revert API: " + e.getMessage());
            return -1; // Return -1 or any other value to indicate an error
        }
    }

    private static boolean revertEditedRecords(String apiEndpoint, String payload) {
        try {
            Response response = RestAssured.given()
                .header("Content-Type", "application/json")
                .body(payload)
                .post(apiEndpoint);

            if (response.getStatusCode() == 200) {
                Logs.info("Revert successful for payload: " + payload);
                return true;
            } else {
                Logs.error("Revert failed with status code: " +
                    response.getStatusCode());
                return false;
            }
        } catch (Exception e) {
            Logs.error("Error during revert API call: " + e.getMessage());
            return false;
        }
    }
}
