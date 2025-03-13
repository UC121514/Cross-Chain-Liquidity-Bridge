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
# Hash 2273
# Hash 5210
# Hash 4586
# Hash 6677
# Hash 9021
# Hash 9259
# Hash 5867
# Hash 7807
# Hash 9017
# Hash 5570
# Hash 2109
# Hash 5921
# Hash 4185
# Hash 7509
# Hash 1688
# Hash 9630
# Hash 1174
# Hash 5701
# Hash 8656
# Hash 4509
# Hash 3282
# Hash 7852
# Hash 5287
# Hash 1478
# Hash 6470
# Hash 2732
# Hash 8648
# Hash 8051
# Hash 8644
# Hash 4370
# Hash 7702
# Hash 1642
# Hash 9381
# Hash 1279
# Hash 9697
# Hash 7960
# Hash 9461
# Hash 9369
# Hash 5326
# Hash 2322
# Hash 4563
# Hash 4175
# Hash 6546
# Hash 3391
# Hash 1430
# Hash 1823
# Hash 6045
# Hash 3115
# Hash 3166
# Hash 2478
# Hash 2542
# Hash 7738
# Hash 9330
# Hash 5161
# Hash 9282
# Hash 1162
# Hash 1566
# Hash 6685
# Hash 7021
# Hash 7579
# Hash 6904
# Hash 7048
# Hash 9237
# Hash 7808
# Hash 7076
# Hash 9353
# Hash 4825
# Hash 3187
# Hash 8877
# Hash 4912
# Hash 1913
# Hash 2021
# Hash 1208
# Hash 5916
# Hash 1652
# Hash 4414
# Hash 9221
# Hash 5412
# Hash 9924
# Hash 8161
# Hash 8573
# Hash 4049