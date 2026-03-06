<?php
// Get the country value from the form submission
$country = isset($_POST['country']) ? htmlspecialchars($_POST['country']) : '';

// Check if the country value is provided
if ($country) {
    // Generate the URL for redirection
    $redirectUrl = $country . '.php';

    // Ensure the file exists before redirecting
    if (file_exists($redirectUrl)) {
        header("Location: $redirectUrl");
        exit();
    } else {
        echo "The page for '$country' does not exist.";
    }
} else {
    echo "No country value provided.";
}
?>
