Feature: Remote File Download from Selenium Grid
  As a test automation engineer
  I want to download files from remote Selenium Grid to local machine
  So that I can verify the downloaded files

  @download
  Scenario: Verify remote file download and copy to local path
    Given I am on the download page
    When I click on the download button
    Then I verify the remote file download and copy to the local path

  @download
  Scenario: Verify specific file download
    Given I am on the reports page
    When I download the monthly report
    Then I verify the remote file download "monthly-report.pdf" and copy to the local path

  @download
  Scenario: Verify large file download with custom timeout
    Given I am on the download page
    When I download the large dataset file
    Then I verify the remote file download and copy to the local path with timeout 120 seconds

  @download
  Scenario: Verify multiple file downloads
    Given I am on the documents page
    When I download invoice file
    And I download receipt file
    Then I verify the remote file download "invoice.pdf" and copy to the local path
    And I verify the remote file download "receipt.pdf" and copy to the local path
