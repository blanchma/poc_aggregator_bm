require 'rubygems'
require 'bundler/setup'
require 'securerandom'
require 'benchmark'
require 'benchmark/memory'
require 'json'
Bundler.require(:default)

$avid = 32800
$redis = Redis.new

def bytes_to_megabytes(bytes)
  bytes / (1024.0 * 1024.0)
end

module MsgPackSerializer

  def serialized
    MessagePack.pack(self.as_json.merge!(uuid: @uuid))
  end

  def merge(devices, item)
    device = MessagePack.unpack(item)
    if devices[ device["data"]["avid"] ]
      devices[ device["data"]["avid"] ]["data"].merge!(device["data"])
    else
      devices[ device["data"]["avid"] ] = device
    end
  end

end

module OjSerializer

  def serialized
    Oj.dump(self.as_json.merge!(uuid: @uuid))
  end

  def merge(devices, item)
    device = Oj.load(item)
    if devices[ device[:data][:avid] ]
      devices[ device[:data][:avid] ][:data].merge!(device[:data])
    else
      devices[ device[:data][:avid] ] = device
    end
  end
end


module JsonSerializer

  def serialized
    self.as_json.merge!(uuid: @uuid).to_json
  end

  def merge(devices, item)
    device = JSON.parse(item)
    if devices[ device["data"]["avid"] ]
      devices[ device["data"]["avid"] ]["data"].merge!(device["data"])
    else
      devices[ device["data"]["avid"] ] = device
    end
  end
end

class Serializer

  attr_accessor :uuid

  def initialize(*args)
    @uuid = SecureRandom.uuid
  end
end


class CreateDevice < Serializer

  attr_accessor :avid, :created_at

  def initialize(location_avid)
    @mac_address = SecureRandom.hex(6)
    @avid = $avid+=1
    @location_avid = location_avid
    @created_at = Time.now
    super
  end


  def as_json
    {
      "location_avid": @location_avid,
      "created_at": @created_at.to_s, #'2020-06-27T05:54:06-03:00'
      "data": {
          "vendor_code": 2,
          "mac_address":  @mac_address,
          "product_code": 3,
          "mesh_status": "available",
          "avid": @avid,
          "name": "GE In Wall Dimmer"
      },
      "type_of_object": "device",
      "channel": "mesh",
      "action": "create"
    }
  end
end

class UpdateDevice < Serializer

  attr_accessor :avid, :created_at

  def initialize(location_avid, avid, update={})
    @avid = $avid
    @location_avid = location_avid
    @update = update
    @created_at = Time.now
    super
  end

  def as_json
    {
      "location_avid": @location_avid,
      "created_at": @created_at.to_s, #'2020-06-27T05:54:06-03:00'
      "data": {
          "avid": @avid,
      }.merge!(@update),
      "type_of_object": "update",
      "channel": "mesh",
      "action": "create"
    }
  end
end

class DeleteDevice < Serializer

  attr_accessor :avid, :created_at

  def initialize(location_avid, avid)
    @avid = $avid
    @location_avid = location_avid
    @created_at = Time.now
    super
  end

  def as_json
    {
      "location_avid": @location_avid,
      "created_at": @created_at.to_s, #'2020-06-27T05:54:06-03:00'
      "data": {
          "avid": @avid,
      },
      "type_of_object": "delete",
      "channel": "mesh",
      "action": "create"
    }
  end
end

class ChangesV1

  attr_accessor :location_avid, :devices, :redis

  def initialize(location_avid, serializer = MsgPackSerializer)
    @location_avid = location_avid
    @redis = Redis.new
    @devices = {}
    @redis.flushdb
    Serializer.include(serializer)
    Serializer.extend(serializer)
    @initial_memory = @redis.info["used_memory"].to_i
  end

  def create_changes(x=100000)
    x.times do
      create_device = CreateDevice.new(location_avid)
      avid = create_device.avid

      redis.zadd "location:#{location_avid}", create_device.created_at.to_i, create_device.serialized

      update_device_1 = UpdateDevice.new(location_avid, avid, {"version" => ["2.1.2"]})

      redis.zadd "location:#{location_avid}", update_device_1.created_at.to_i, update_device_1.serialized

      update_device_2 = UpdateDevice.new(location_avid, avid, {"name" => "New Name #{avid}"})

      redis.zadd "location:#{location_avid}", update_device_2.created_at.to_i, update_device_2.serialized
    end
  end

  def fetch_and_merge
    while items = redis.zrevrange("location:#{location_avid}", index||=0, index+= 100000) and not(items.empty?) do
      #puts "start index: #{index} items: #{items.size} "
      items.each do |item|
        Serializer.merge(devices, item)
      end
      #puts "finish index: #{index}"
    end
  end

  def bytes_to_megabytes(bytes)
    bytes / (1024.0 * 1024.0)
  end

  def memory_usage
    bytes_to_megabytes(@redis.info["used_memory"].to_i - @initial_memory)
  end
end

sleep 5

Benchmark.bm do |benchmark|

  changes_v1 = ChangesV1.new(1, MsgPackSerializer)

  q = 50000

  create_time = benchmark.report "create" do
    changes_v1.create_changes(q)
  end

  puts "create/ms: #{(create_time.real / q * 3) * 1000}ms"

  merge_time = benchmark.report "merge" do
    changes_v1.fetch_and_merge
  end

  puts "merge/ms: #{(merge_time.real / q * 3) * 1000}ms"

  puts "\nCount devices after merge: #{changes_v1.devices.size}. Memory: #{changes_v1.memory_usage}"
end
