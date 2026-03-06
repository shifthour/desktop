<?php

$startTime = strtotime(date("Y-m-d 00:00:00"));
$endTime = time();
$currentTime = time();
$currentTimeStamp = date("h:i:s", $currentTime);
$key = '...';
$id = '....';
$hash = hash_hmac('sha256', $currentTime, $key);

$url = "https://pahal.housing.com/api/v0/get-builder-leads?start_date=".$startTime."&end_date=".$endTime."&current_time=".$currentTime."&hash=".$hash."&id=".$id;

print($url);
$curl = curl_init();

curl_setopt_array($curl, array(
CURLOPT_URL => $url,
CURLOPT_RETURNTRANSFER => true,
CURLOPT_ENCODING => "",
CURLOPT_MAXREDIRS => 10,
CURLOPT_TIMEOUT => 30,
CURLOPT_SSL_VERIFYHOST => 0,
CURLOPT_SSL_VERIFYPEER => 0,
CURLOPT_HTTP_VERSION => CURL_HTTP_VERSION_1_1,
CURLOPT_CUSTOMREQUEST => "GET",
));
		
$response = curl_exec($curl);
$err = curl_error($curl);

curl_close($curl);

print $response;

?>