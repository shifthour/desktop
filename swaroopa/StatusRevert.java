package com.cao.pageObjects.FCTLocator;

import baseFiles.BaseConfig;
import commonUtilities.Logs;
import io.restassured.response.Response;
import io.restassured.RestAssured;
import org.json.JSONObject;
import org.openqa.selenium.Cookie;
import org.openqa.selenium.WebDriver;
import org.json.JSONArray;
import org.openqa.selenium.logging.LogEntries;
import org.openqa.selenium.logging.LogEntry;
import org.openqa.selenium.logging.LogType;
import org.openqa.selenium.JavascriptExecutor;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class StatusRevert {
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
    private static boolean isCapturing = false;

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

    public static void enableNetworkCapture(WebDriver driver) {
        try {
            Logs.info("Enabling network capture with JavaScript interceptor...");
            isCapturing = true;

            // Clear previous captured payloads
            int previousSize = capturedPayloads.size();
            capturedPayloads.clear();
            if (previousSize > 0) {
                Logs.info("Cleared " + previousSize + " previous captured payloads");
            }

            // Inject JavaScript to intercept requests
            JavascriptExecutor js = (JavascriptExecutor) driver;
            String interceptorScript =
                "window.capturedRequests = [];" +
                "(function() {" +
                "  var origOpen = XMLHttpRequest.prototype.open;" +
                "  var origSend = XMLHttpRequest.prototype.send;" +
                "  XMLHttpRequest.prototype.open = function(method, url) {" +
                "    this._url = url; this._method = method;" +
                "    return origOpen.apply(this, arguments);" +
                "  };" +
                "  XMLHttpRequest.prototype.send = function(data) {" +
                "    console.log('[XHR] Request to: ' + this._url + ', has data: ' + !!data);" +
                "    if (this._url && this._url.includes('bulkEditAdmin') && data) {" +
                "      console.log('[XHR] CAPTURED bulkEditAdmin request!');" +
                "      window.capturedRequests.push({url: this._url, data: data});" +
                "    }" +
                "    return origSend.apply(this, arguments);" +
                "  };" +
                "  var origFetch = window.fetch;" +
                "  window.fetch = function(url, opts) {" +
                "    var urlStr = url.toString ? url.toString() : url;" +
                "    console.log('[FETCH] Request to: ' + urlStr);" +
                "    if (urlStr.includes('bulkEditAdmin') && opts && opts.body) {" +
                "      console.log('[FETCH] CAPTURED bulkEditAdmin request!');" +
                "      window.capturedRequests.push({url: urlStr, data: opts.body});" +
                "    }" +
                "    return origFetch.apply(this, arguments);" +
                "  };" +
                "  console.log('[INTERCEPTOR] JavaScript interceptor loaded successfully');" +
                "})();";
            js.executeScript(interceptorScript);
            Logs.info("JavaScript interceptor injected successfully.");
        } catch (Exception e) {
            Logs.error("Failed to enable network capture: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Failed to enable network capture", e);
        }
    }

    public static void disableNetworkCapture(WebDriver driver) {
        if (!isCapturing) {
            Logs.error("Network capture is not enabled.");
            return;
        }

        try {
            Logs.info("Waiting 2 seconds for requests to be captured...");
            Thread.sleep(2000);

            // Retrieve captured requests from JavaScript
            JavascriptExecutor js = (JavascriptExecutor) driver;
            Object capturedRequestsObj = js.executeScript("return window.capturedRequests || [];");

            if (capturedRequestsObj == null) {
                Logs.warn("No captured requests found!");
                isCapturing = false;
                return;
            }

            @SuppressWarnings("unchecked")
            ArrayList<Map<String, Object>> capturedRequests =
                (ArrayList<Map<String, Object>>) capturedRequestsObj;

            Logs.info("Retrieved " + capturedRequests.size() + " captured requests");

            if (capturedRequests.isEmpty()) {
                Logs.warn("No bulkEditAdmin requests were captured!");
                Logs.warn("Check browser console for [XHR] and [FETCH] logs to see all requests");

                // Try to read browser console logs
                try {
                    LogEntries browserLogs = driver.manage().logs().get(LogType.BROWSER);
                    Logs.info("===== BROWSER CONSOLE LOGS =====");
                    for (LogEntry entry : browserLogs) {
                        String message = entry.getMessage();
                        if (message.contains("[XHR]") || message.contains("[FETCH]") ||
                            message.contains("[INTERCEPTOR]") || message.contains("bulkEditAdmin")) {
                            Logs.info("Browser Console: " + message);
                        }
                    }
                    Logs.info("================================");
                } catch (Exception e) {
                    Logs.info("Could not read browser console logs: " + e.getMessage());
                }
            }

            for (Map<String, Object> request : capturedRequests) {
                String url = (String) request.get("url");
                String data = (String) request.get("data");

                Logs.info("Processing captured request: " + url);
                Logs.info("Data (first 300 chars): " +
                    (data != null ? data.substring(0, Math.min(300, data.length())) : "null"));

                if (data != null && !data.isEmpty()) {
                    List<String> fcids = extractFCIds(data);
                    if (!fcids.isEmpty()) {
                        capturedPayloads.addAll(fcids);
                        Logs.info("Extracted " + fcids.size() + " FCIDs: " + fcids);
                    }
                }
            }

            // Clear captured requests in browser
            js.executeScript("window.capturedRequests = [];");

            Logs.info("========== NETWORK CAPTURE SUMMARY ==========");
            Logs.info("Total requests captured: " + capturedRequests.size());
            Logs.info("Total FCIDs captured: " + capturedPayloads.size());
            Logs.info("Captured FCID list: " + capturedPayloads);
            Logs.info("============================================");

            isCapturing = false;
        } catch (Exception e) {
            Logs.error("Failed to disable network capture: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static List<String> extractFCIds(String postData) {
        List<String> fcids = new ArrayList<>();
        Logs.info("Attempting to extract FCIDs from POST data...");
        Logs.info("POST data sample (first 500 chars): " +
            postData.substring(0, Math.min(500, postData.length())));

        var matcher =
            java.util.regex.Pattern.compile("\"fcId\":\"(.+?)\"").matcher(postData);
        int matchCount = 0;
        while (matcher.find()) {
            matchCount++;
            String fcid = matcher.group(1);
            fcids.add(fcid);
            Logs.info("Extracted FCID #" + matchCount + ": " + fcid);
        }

        if (matchCount == 0) {
            Logs.warn("No FCID pattern matches found in POST data!");
            Logs.info("Full POST data: " + postData);
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
        Logs.info("Building revert payload for FCID: " + fcid);
        Logs.info("Initial values map: " + initialValues);
        Logs.info("Updated values map: " + updatedValues);

        StringBuilder json = new StringBuilder("{");
        json.append("\"fcId\":\"").append(fcid).append("\"");

        int fieldsAdded = 0;
        for (String field : List.of("status", "comments", "reason", "date")) {
            String initialValue = initialValues.get(field);
            String updatedValue = updatedValues.get(field);

            Logs.info("Processing field '" + field + "': initial=" + initialValue +
                ", updated=" + updatedValue);

            if (updatedValue != null && !updatedValue.equalsIgnoreCase(initialValue)) {
                json.append(",");
                json.append("\"").append(field).append("\":{\"from\":\"")
                    .append(updatedValue).append("\",\"to\":\"")
                    .append(initialValue).append("\"}");
                fieldsAdded++;
                Logs.info("Added field '" + field + "' to payload: from=" + updatedValue +
                    " to=" + initialValue);
            } else {
                Logs.info("Skipping field '" + field + "' (no change or null)");
            }
        }
        json.append("}");

        Logs.info("Payload built with " + fieldsAdded + " changed fields");
        return json.toString();
    }

    public static void revertEditedRecords() {
        Logs.info("========== STARTING REVERT PROCESS ==========");
        Logs.info("Total captured payloads: " + capturedPayloads.size());
        Logs.info("Captured FCID list: " + capturedPayloads);

        if (capturedPayloads.isEmpty()) {
            Logs.warn("No captured payloads to revert. Skipping revert process.");
            Logs.info("Initial values: " + initialValues);
            Logs.info("Updated values: " + updatedValues);
            return;
        }

        String baseUrl = properties.getProperty("base.url");
        String queryEndpoint = properties.getProperty("query.endpoint");
        String fullApiUrl = baseUrl + queryEndpoint;

        Logs.info("API Configuration - Base URL: " + baseUrl);
        Logs.info("API Configuration - Endpoint: " + queryEndpoint);
        Logs.info("Full API URL: " + fullApiUrl);

        int successCount = 0;
        int failureCount = 0;

        for (String fcid : capturedPayloads) {
            Logs.info("---------- Processing FCID: " + fcid + " ----------");
            String payload = buildRevertPayload(fcid);
            Logs.info("Built revert payload: " + payload);

            int responseCode = callRevertAPI(fullApiUrl, payload);
            response.put(fcid, String.valueOf(responseCode));

            if (responseCode >= 200 && responseCode < 300) {
                successCount++;
                Logs.info("SUCCESS - FCID " + fcid + " reverted with response code: " +
                    responseCode);
            } else {
                failureCount++;
                Logs.error("FAILURE - FCID " + fcid + " revert failed with response code: " +
                    responseCode);
            }
        }

        Logs.info("========== REVERT SUMMARY ==========");
        Logs.info("Total FCIDs processed: " + capturedPayloads.size());
        Logs.info("Successful reverts: " + successCount);
        Logs.info("Failed reverts: " + failureCount);
        Logs.info("====================================");
    }

    public static int callRevertAPI(String apiUrl, String payload) {
        Logs.info("Calling revert API...");
        Logs.info("API URL: " + apiUrl);
        Logs.info("Payload: " + payload);

        try {
            URL url = new URL(apiUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setDoOutput(true);

            Logs.info("Sending POST request...");
            connection.getOutputStream().write(payload.getBytes(StandardCharsets.UTF_8));

            int code = connection.getResponseCode();
            String responseMessage = connection.getResponseMessage();

            Logs.info("Response Code: " + code);
            Logs.info("Response Message: " + responseMessage);

            connection.disconnect();
            return code;
        } catch (IOException e) {
            Logs.error("Failed to call revert API: " + e.getMessage());
            e.printStackTrace();
            return -1;
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
