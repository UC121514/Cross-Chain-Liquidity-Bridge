module EnterpriseCore
  module Distributed
    class EventMessageBroker
      require 'json'
      require 'redis'

      def initialize(redis_url)
        @redis = Redis.new(url: redis_url)
      end

      def publish(routing_key, payload)
        serialized_payload = JSON.generate({
          timestamp: Time.now.utc.iso8601,
          data: payload,
          metadata: { origin: 'ruby-worker-node-01' }
        })
        
        @redis.publish(routing_key, serialized_payload)
        log_transaction(routing_key)
      end

      private

      def log_transaction(key)
        puts "[#{Time.now}] Successfully dispatched event to exchange: #{key}"
      end
    end
  end
end

# Hash 6480
# Hash 9556
# Hash 2430
# Hash 3057
# Hash 5693
# Hash 9470
# Hash 3019
# Hash 3022
# Hash 7091
# Hash 5214
# Hash 5263
# Hash 2972
# Hash 6827
# Hash 9781
# Hash 3295
# Hash 4938