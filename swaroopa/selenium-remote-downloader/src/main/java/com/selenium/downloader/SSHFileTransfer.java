package com.selenium.downloader;

import com.jcraft.jsch.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * SSH/SCP based file transfer from remote Grid node to local machine
 * Requires JSch library dependency
 *
 * Add to pom.xml:
 * <dependency>
 *     <groupId>com.jcraft</groupId>
 *     <artifactId>jsch</artifactId>
 *     <version>0.1.55</version>
 * </dependency>
 */
public class SSHFileTransfer {

    private final String remoteHost;
    private final int remotePort;
    private final String remoteUser;
    private final String remotePassword;
    private final String privateKeyPath;
    private final String remoteDownloadPath;
    private final String localDownloadPath;
    private final long timeoutSeconds;

    private SSHFileTransfer(Builder builder) {
        this.remoteHost = builder.remoteHost;
        this.remotePort = builder.remotePort;
        this.remoteUser = builder.remoteUser;
        this.remotePassword = builder.remotePassword;
        this.privateKeyPath = builder.privateKeyPath;
        this.remoteDownloadPath = builder.remoteDownloadPath;
        this.localDownloadPath = builder.localDownloadPath;
        this.timeoutSeconds = builder.timeoutSeconds;

        try {
            Files.createDirectories(Paths.get(localDownloadPath));
        } catch (IOException e) {
            throw new RuntimeException("Failed to create local download directory", e);
        }
    }

    /**
     * Wait for file to be downloaded on remote machine and copy it to local
     */
    public String downloadFile(String expectedFileName) throws Exception {
        System.out.println("Connecting to remote host: " + remoteHost);

        JSch jsch = new JSch();

        // Add private key if provided
        if (privateKeyPath != null && !privateKeyPath.isEmpty()) {
            jsch.addIdentity(privateKeyPath);
        }

        Session session = null;
        ChannelSftp sftpChannel = null;

        try {
            // Create session
            session = jsch.getSession(remoteUser, remoteHost, remotePort);

            if (remotePassword != null && !remotePassword.isEmpty()) {
                session.setPassword(remotePassword);
            }

            // Disable strict host key checking (use with caution)
            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);

            session.connect(30000); // 30 seconds timeout
            System.out.println("SSH Session connected");

            // Wait for file to appear
            String remoteFilePath = waitForRemoteFile(session, expectedFileName);

            // Open SFTP channel
            Channel channel = session.openChannel("sftp");
            channel.connect();
            sftpChannel = (ChannelSftp) channel;

            // Download file
            String localFilePath = Paths.get(localDownloadPath, expectedFileName).toString();
            sftpChannel.get(remoteFilePath, localFilePath);

            System.out.println("File downloaded successfully to: " + localFilePath);
            return localFilePath;

        } finally {
            if (sftpChannel != null && sftpChannel.isConnected()) {
                sftpChannel.disconnect();
            }
            if (session != null && session.isConnected()) {
                session.disconnect();
            }
        }
    }

    /**
     * Wait for file to appear on remote machine
     */
    private String waitForRemoteFile(Session session, String expectedFileName) throws Exception {
        long startTime = System.currentTimeMillis();
        long maxWaitTime = timeoutSeconds * 1000;

        while (System.currentTimeMillis() - startTime < maxWaitTime) {
            try {
                // Execute ls command on remote machine
                String command = String.format("ls -1 %s | grep %s",
                    remoteDownloadPath, expectedFileName);

                String output = executeCommand(session, command);

                if (output != null && !output.trim().isEmpty()) {
                    String fileName = output.trim().split("\n")[0];
                    String fullPath = Paths.get(remoteDownloadPath, fileName).toString();
                    System.out.println("Found remote file: " + fullPath);
                    return fullPath;
                }

            } catch (Exception e) {
                System.out.println("Waiting for file on remote... " + e.getMessage());
            }

            Thread.sleep(1000);
        }

        throw new Exception("File not found on remote machine within timeout: " + expectedFileName);
    }

    /**
     * Execute command on remote machine via SSH
     */
    private String executeCommand(Session session, String command) throws Exception {
        ChannelExec execChannel = null;
        try {
            execChannel = (ChannelExec) session.openChannel("exec");
            execChannel.setCommand(command);

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            execChannel.setOutputStream(outputStream);
            execChannel.setErrStream(System.err);

            execChannel.connect();

            // Wait for command to complete
            while (!execChannel.isClosed()) {
                Thread.sleep(100);
            }

            return outputStream.toString();

        } finally {
            if (execChannel != null && execChannel.isConnected()) {
                execChannel.disconnect();
            }
        }
    }

    /**
     * Builder pattern for SSHFileTransfer
     */
    public static class Builder {
        private String remoteHost;
        private int remotePort = 22;
        private String remoteUser;
        private String remotePassword;
        private String privateKeyPath;
        private String remoteDownloadPath = "/tmp";
        private String localDownloadPath;
        private long timeoutSeconds = 60;

        public Builder remoteHost(String remoteHost) {
            this.remoteHost = remoteHost;
            return this;
        }

        public Builder remotePort(int remotePort) {
            this.remotePort = remotePort;
            return this;
        }

        public Builder remoteUser(String remoteUser) {
            this.remoteUser = remoteUser;
            return this;
        }

        public Builder remotePassword(String remotePassword) {
            this.remotePassword = remotePassword;
            return this;
        }

        public Builder privateKeyPath(String privateKeyPath) {
            this.privateKeyPath = privateKeyPath;
            return this;
        }

        public Builder remoteDownloadPath(String remoteDownloadPath) {
            this.remoteDownloadPath = remoteDownloadPath;
            return this;
        }

        public Builder localDownloadPath(String localDownloadPath) {
            this.localDownloadPath = localDownloadPath;
            return this;
        }

        public Builder timeoutSeconds(long timeoutSeconds) {
            this.timeoutSeconds = timeoutSeconds;
            return this;
        }

        public SSHFileTransfer build() {
            Objects.requireNonNull(remoteHost, "Remote host is required");
            Objects.requireNonNull(remoteUser, "Remote user is required");
            Objects.requireNonNull(localDownloadPath, "Local download path is required");

            if ((remotePassword == null || remotePassword.isEmpty()) &&
                (privateKeyPath == null || privateKeyPath.isEmpty())) {
                throw new IllegalArgumentException("Either password or private key must be provided");
            }

            return new SSHFileTransfer(this);
        }
    }
}
