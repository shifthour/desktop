<?php

error_reporting(E_ALL);
ini_set('display_errors', 1);

// Include PHPMailer files
require 'PHPMailer/src/Exception.php';
require 'PHPMailer/src/PHPMailer.php';
require 'PHPMailer/src/SMTP.php';

use PHPMailer\PHPMailer\PHPMailer;
use PHPMailer\PHPMailer\Exception;

$mail = new PHPMailer(true);

try {
    // Server settings
    $mail->isSMTP();                                      // Send using SMTP
    $mail->Host       = 'smtp.gmail.com';                 // Set the SMTP server to send through
    $mail->SMTPAuth   = true;                             // Enable SMTP authentication
    $mail->Username   = 'sales@ishtikahomes.com';         // SMTP username (Google Workspace email)
    $mail->Password   = 'Agastya123';                     // SMTP password (Use an App Password, not your actual Google account password)
    $mail->SMTPSecure = PHPMailer::ENCRYPTION_STARTTLS;   // Enable TLS encryption
    $mail->Port       = 587;                              // TCP port to connect to (587 for TLS)

    // Recipients
    $mail->setFrom('sales@ishtikahomes.com', 'Ishtika Homes');
    $mail->addAddress('sales@ishtikahomes.com');        // Add a recipient
    $mail->addAddress('rahul.chitala1818@gmail.com');
    
    // $mail->addReplyTo($_POST['email'], $_POST['name']); 

    // Content
    $mail->isHTML(false);                                 // Set email format to plain text
    $mail->Subject = 'Contact Us Page';

    // Form fields
    $name = $_POST['name'];
    $name2 = $_POST['name2'];
    $email = $_POST['email'];
    $phone = $_POST['phone'];
    $msg_subject = $_POST['subject'];
    $message = $_POST['message'];

    // Email body content
    $body = $name . " " . $name2 . " has messaged." . "\n\n" .
            "Email: " . $email . "\n\n" .
            "Phone: " . $phone . "\n\n" .
            "Subject: " . $msg_subject . "\n\n" .
            "Message: " . $message . "\n\n";
    $mail->Body = $body;

    // Send email
    $mail->send();

    // Success alert
    echo '<script language="javascript" type="text/javascript">';
    echo 'alert("Thank you for the message. We will contact you shortly.");';
    echo 'window.location.href = "index.php";';
    echo '</script>';

} catch (Exception $e) {
    // Error alert
    echo '<script language="javascript" type="text/javascript">';
    echo 'alert("Message could not be sent. Mailer Error: ' . addslashes($mail->ErrorInfo) . '");';
    echo 'window.location.href = "index.php";';
    echo '</script>';
}
?>
