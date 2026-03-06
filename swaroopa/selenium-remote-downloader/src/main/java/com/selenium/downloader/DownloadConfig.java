package com.selenium.downloader;

import java.io.*;
import java.util.*;

/**
 * Configuration class for remote file download settings
 */
public class DownloadConfig {

    private String localDownloadPath;
    private String remoteDownloadPath;
    private long timeoutSeconds;
    private DownloadMethod downloadMethod;

    // SSH specific config
    private String sshHost;
    private int sshPort;
    private String sshUser;
    private String sshPassword;
    private String sshPrivateKeyPath;

    public enum DownloadMethod {
        SELENIUM_4_API,  // Use Selenium 4's downloadable files API
        SSH_SCP,         // Use SSH/SCP to copy files
        CDP_PROTOCOL     // Use Chrome DevTools Protocol
    }

    private DownloadConfig(Builder builder) {
        this.localDownloadPath = builder.localDownloadPath;
        this.remoteDownloadPath = builder.remoteDownloadPath;
        this.timeoutSeconds = builder.timeoutSeconds;
        this.downloadMethod = builder.downloadMethod;
        this.sshHost = builder.sshHost;
        this.sshPort = builder.sshPort;
        this.sshUser = builder.sshUser;
        this.sshPassword = builder.sshPassword;
        this.sshPrivateKeyPath = builder.sshPrivateKeyPath;
    }

    // Getters
    public String getLocalDownloadPath() { return localDownloadPath; }
    public String getRemoteDownloadPath() { return remoteDownloadPath; }
    public long getTimeoutSeconds() { return timeoutSeconds; }
    public DownloadMethod getDownloadMethod() { return downloadMethod; }
    public String getSshHost() { return sshHost; }
    public int getSshPort() { return sshPort; }
    public String getSshUser() { return sshUser; }
    public String getSshPassword() { return sshPassword; }
    public String getSshPrivateKeyPath() { return sshPrivateKeyPath; }

    /**
     * Load configuration from properties file
     */
    public static DownloadConfig fromPropertiesFile(String filePath) throws IOException {
        Properties props = new Properties();
        try (InputStream input = new FileInputStream(filePath)) {
            props.load(input);
        }

        Builder builder = new Builder()
            .localDownloadPath(props.getProperty("local.download.path", "./downloads"))
            .remoteDownloadPath(props.getProperty("remote.download.path", "/tmp"))
            .timeoutSeconds(Long.parseLong(props.getProperty("timeout.seconds", "60")))
            .downloadMethod(DownloadMethod.valueOf(
                props.getProperty("download.method", "SELENIUM_4_API")));

        // SSH config if provided
        if (props.containsKey("ssh.host")) {
            builder.sshHost(props.getProperty("ssh.host"))
                   .sshPort(Integer.parseInt(props.getProperty("ssh.port", "22")))
                   .sshUser(props.getProperty("ssh.user"))
                   .sshPassword(props.getProperty("ssh.password"))
                   .sshPrivateKeyPath(props.getProperty("ssh.private.key.path"));
        }

        return builder.build();
    }

    /**
     * Builder pattern
     */
    public static class Builder {
        private String localDownloadPath = "./downloads";
        private String remoteDownloadPath = "/tmp";
        private long timeoutSeconds = 60;
        private DownloadMethod downloadMethod = DownloadMethod.SELENIUM_4_API;
        private String sshHost;
        private int sshPort = 22;
        private String sshUser;
        private String sshPassword;
        private String sshPrivateKeyPath;

        public Builder localDownloadPath(String path) {
            this.localDownloadPath = path;
            return this;
        }

        public Builder remoteDownloadPath(String path) {
            this.remoteDownloadPath = path;
            return this;
        }

        public Builder timeoutSeconds(long seconds) {
            this.timeoutSeconds = seconds;
            return this;
        }

        public Builder downloadMethod(DownloadMethod method) {
            this.downloadMethod = method;
            return this;
        }

        public Builder sshHost(String host) {
            this.sshHost = host;
            return this;
        }

        public Builder sshPort(int port) {
            this.sshPort = port;
            return this;
        }

        public Builder sshUser(String user) {
            this.sshUser = user;
            return this;
        }

        public Builder sshPassword(String password) {
            this.sshPassword = password;
            return this;
        }

        public Builder sshPrivateKeyPath(String path) {
            this.sshPrivateKeyPath = path;
            return this;
        }

        public DownloadConfig build() {
            Objects.requireNonNull(localDownloadPath, "Local download path is required");
            return new DownloadConfig(this);
        }
    }

    @Override
    public String toString() {
        return "DownloadConfig{" +
            "localDownloadPath='" + localDownloadPath + '\'' +
            ", remoteDownloadPath='" + remoteDownloadPath + '\'' +
            ", timeoutSeconds=" + timeoutSeconds +
            ", downloadMethod=" + downloadMethod +
            ", sshHost='" + sshHost + '\'' +
            ", sshPort=" + sshPort +
            ", sshUser='" + sshUser + '\'' +
            '}';
    }
}
