package com.automation.steps;

import com.automation.utils.RemoteFileCopy;
import io.cucumber.java.en.When;

/**
 * Step Definition for Remote File Copy
 */
public class RemoteFileCopySteps {

    @When("I copy the remote file to local machine")
    public void iCopyTheRemoteFileToLocalMachine() throws Exception {
        RemoteFileCopy.copyLatestFile();
    }

    @When("I copy the remote file {string} to local machine")
    public void iCopyTheRemoteFileToLocalMachine(String fileName) throws Exception {
        RemoteFileCopy.copyFile(fileName);
    }
}
