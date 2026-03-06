require 'openssl'
require 'date'
 require 'uri'
require 'net/http'


def getHash
	t = DateTime.now.strftime('%s')
	k = '...'
    x = OpenSSL::HMAC.hexdigest(OpenSSL::Digest.new('sha256'), k , t)
    puts x
    puts t
  start_time = '1527951824' 
  end_time =   '1528097403'
  id = '..' 
    url_string = "https://pahal.housing.com/api/v0/get-builder-leads?start_date="+ start_time+ "&end_date=" + end_time+ "&current_time="+ t +"&hash=" + x + "&id=" + id
  puts url_string
	url = URI(url_string)
	http = Net::HTTP.new(url.host, url.port)
	http.use_ssl = true
	http.verify_mode = OpenSSL::SSL::VERIFY_NONE

	request = Net::HTTP::Get.new(url)
	request["cache-control"] = 'no-cache'

	response = http.request(request)
	puts response.read_body
end 


getHash
