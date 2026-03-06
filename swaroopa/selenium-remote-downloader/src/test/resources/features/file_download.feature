Feature: Remote File Download Verification
  As a test automation engineer
  I want to download files from remote Selenium Grid to my local machine
  So that I can verify the downloaded files in my tests

  Background:
    Given the remote file download is configured

  @download @smoke
  Scenario: Verify single file download from remote machine
    Given I am on the download page
    When I click on download button
    Then verify file download on the remote machine

  @download
  Scenario: Verify PDF report download
    Given I am on the reports page
    When I generate monthly report
    And I click on download PDF button
    Then verify file download on the remote machine with filename "monthly-report.pdf"

  @download
  Scenario: Verify multiple files download
    Given I am on the documents page
    When I download the following files:
      | invoice.pdf  |
      | receipt.pdf  |
      | summary.xlsx |
    Then verify all files are downloaded on the remote machine

  @download
  Scenario Outline: Verify file download with different formats
    Given I am on the export page
    When I select "<format>" export option
    And I click on export button
    Then verify file download on the remote machine with extension "<extension>"

    Examples:
      | format | extension |
      | PDF    | .pdf      |
      | Excel  | .xlsx     |
      | CSV    | .csv      |

  @download @timeout
  Scenario: Verify large file download with custom timeout
    Given I am on the download page
    When I download large dataset file
    Then verify file download on the remote machine with timeout 300 seconds

  @download @negative
  Scenario: Verify download failure handling
    Given I am on the download page
    When I trigger invalid download
    Then verify download fails gracefully with error message
