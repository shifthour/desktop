package com.automation.steps;

import com.automation.hooks.Hooks;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.When;
import org.openqa.selenium.By;
import org.openqa.selenium.remote.RemoteWebDriver;

/**
 * Supporting Navigation and Action Steps
 * Customize these for your application
 */
public class NavigationSteps {

    private final RemoteWebDriver driver;

    public NavigationSteps() {
        this.driver = Hooks.getDriver();
    }

    // ==================== GIVEN Steps ====================

    @Given("I am on the download page")
    public void iAmOnTheDownloadPage() {
        String url = System.getProperty("download.page.url", "https://example.com/download");
        driver.get(url);
        System.out.println("Navigated to download page: " + url);
    }

    @Given("I am on the reports page")
    public void iAmOnTheReportsPage() {
        String url = System.getProperty("reports.page.url", "https://example.com/reports");
        driver.get(url);
        System.out.println("Navigated to reports page: " + url);
    }

    @Given("I am on the documents page")
    public void iAmOnTheDocumentsPage() {
        String url = System.getProperty("documents.page.url", "https://example.com/documents");
        driver.get(url);
        System.out.println("Navigated to documents page: " + url);
    }

    // ==================== WHEN Steps ====================

    @When("I click on the download button")
    public void iClickOnTheDownloadButton() {
        try {
            // Customize this locator for your application
            driver.findElement(By.id("download-button")).click();
            System.out.println("Clicked download button");
            Thread.sleep(1000); // Wait for download to initiate
        } catch (Exception e) {
            System.err.println("Failed to click download button: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @When("I download the monthly report")
    public void iDownloadTheMonthlyReport() {
        try {
            driver.findElement(By.id("monthly-report-download")).click();
            System.out.println("Clicked monthly report download");
            Thread.sleep(1000);
        } catch (Exception e) {
            System.err.println("Failed to download monthly report: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @When("I download the large dataset file")
    public void iDownloadTheLargeDatasetFile() {
        try {
            driver.findElement(By.id("large-file-download")).click();
            System.out.println("Clicked large dataset file download");
            Thread.sleep(2000);
        } catch (Exception e) {
            System.err.println("Failed to download large file: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @When("I download invoice file")
    public void iDownloadInvoiceFile() {
        try {
            driver.findElement(By.id("download-invoice")).click();
            System.out.println("Clicked invoice download");
            Thread.sleep(1000);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @When("I download receipt file")
    public void iDownloadReceiptFile() {
        try {
            driver.findElement(By.id("download-receipt")).click();
            System.out.println("Clicked receipt download");
            Thread.sleep(1000);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
