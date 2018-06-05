require 'kafka'
require 'rest-client'
require 'json'

kafka = Kafka.new(['localhost:29092'], client_id: 'meetup')

producer = kafka.async_producer(delivery_interval: 5)

processed = 0

skip_to = ARGV[1] ? ARGV[1].to_i : 0

kafka.each_message(topic: 'meetup') do |msg|
  if msg.offset <= skip_to
    if msg.offset % 500 == 0
      print(".#{msg.offset}.")
    end
    next
  end

  content = JSON.parse(msg.value)
  if content['group']['group_country'] != 'us'
    puts("skip #{content['group']['group_country']}...")
    next
  end

  if !content['venue'].is_a?(Hash)
    puts("skip no venue, offset: #{msg.offset}...")
    next
  end

  if !content['venue']['lat'].is_a?(Numeric) ||
    !content['venue']['lon'].is_a?(Numeric)
    puts("skip invalid coordinates: #{content['venue']}...")
    next
  end

  query = "https://api.darksky.net/forecast/#{ARGV[0]}/" +
    "#{content['venue']['lat']},#{content['venue']['lon']},#{content['mtime']/1000}"

  puts("query: #{query}")

  retry_count = 0
  result = {}

  loop do
    result = RestClient.get(query)
    break if result.code == 200

    retry_count +=1
    break if retry_count > 3
    puts("code #{result.code}, pause 1s before retry, retried: #{retry_count}")
    sleep 1
  end

  if retry_count > 3
    puts("failed, offset: #{msg.offset}")
    puts("query: #{query}")
    puts("code: #{result.code}")
    puts("body: #{result.body}")
    break
  end

  parsed = JSON.parse(result.body)
  parsed['meetup_rsvp_id'] = content['rsvp_id']

  producer.produce(parsed.to_json, topic: 'meetup_weather_ref')

  processed +=1
  puts(result.body)
  puts("processed offset: #{msg.offset}")
  puts("processed: #{processed}")

  if processed % 400 == 0
    puts("Pausing 1 second for every 400 records")
    sleep 1
  end

end
