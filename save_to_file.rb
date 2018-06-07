require 'kafka'
require 'json'

kafka = Kafka.new(['localhost:29092'], client_id: ARGV[0])

count = 0

File.open(ARGV[1], 'w') do |file|
  kafka.each_message(topic: ARGV[0]) do |msg|
    m = JSON.parse(msg.value)
    line =
      "\"#{m['timestamp']}\",\"#{m['country']}\",\"#{m['city']}\"," \
      "\"#{m['topic']}\",#{m['avg_tmp']},#{m['avg_pres']},#{m['avg_wind']}," \
      "#{m['avg_humi']},#{m['count']}\n"
    file.write(line)
    count += 1
    if count % 1000 == 0
      print("..#{count}.")
    end
  end
end
