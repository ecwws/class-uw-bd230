require 'websocket-client-simple'
require 'kafka'

kafka = Kafka.new(['localhost:29092'], client_id: 'meetup')

producer = kafka.async_producer(delivery_interval: 5)

ws = WebSocket::Client::Simple.connect 'ws://stream.meetup.com/2/rsvps'

ws.on :message do |msg|
  producer.produce(msg, topic: 'meetup')
  puts(msg)
end

loop do
  sleep 10
end
