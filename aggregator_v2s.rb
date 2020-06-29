require 'rubygems'
require 'bundler/setup'
require 'securerandom'
require 'benchmark'
require 'benchmark/memory'
require 'json'
Bundler.require(:default)

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
    #puts "\n"
    #puts devices[ device["data"]["avid"] ]
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

  def id
    "device:#{avid}:uuid:#{uuid}"
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
    @avid = avid
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
    @avid = avid
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

class ChangesV2

  attr_accessor :location_avid, :devices, :redis

  def initialize(location_avid, serializer = MsgPackSerializer)
    @location_avid = location_avid
    @redis = Redis.new
    @devices = {}
    @redis.flushdb
    Serializer.include(serializer)
    Serializer.extend(serializer)
    @initial_memory = @redis.info["used_memory"].to_i
    $avid=32800
  end

  def create_changes(x=100000)
    x.times do
      create_device = CreateDevice.new(location_avid)
      avid = create_device.avid

      redis.zadd "location:#{location_avid}:uuids", create_device.created_at.to_i, create_device.id
      redis.hset "location:#{location_avid}:changes", create_device.id, create_device.serialized

      update_device_1 = UpdateDevice.new(location_avid, avid, {"version" => ["2.1.2"]})

      redis.zadd "location:#{location_avid}:uuids", update_device_1.created_at.to_i, update_device_1.id
      redis.hset "location:#{location_avid}:changes", update_device_1.id, update_device_1.serialized

      update_device_2 = UpdateDevice.new(location_avid, avid, {"name" => "New Name #{avid}"})

      redis.zadd "location:#{location_avid}:uuids", update_device_2.created_at.to_i, update_device_2.id
      redis.hset "location:#{location_avid}:changes", update_device_2.id, update_device_2.serialized
    end
  end

  def fetch_and_merge
    while uuids = redis.zrevrange("location:#{location_avid}:uuids", index||=0, index+= 1000) and not(uuids.empty?) do
      uuids.each do |uuid|
        item = redis.hget "location:#{location_avid}:changes", uuid
        Serializer.merge(devices, item)
      end
      #puts "finish index: #{index}"
    end
  end

  def bytes_to_megabytes(bytes)
    bytes / (1024.0 * 1024.0)
  end

  def find(avid)
    items = []
    redis.zscan_each("location:#{location_avid}:uuids", match: "device:#{avid}:*", count: 1000) do |key|
      items.push(key.first)#.map(&:first))
    end
    # cursor = 0
    # loop do
    #   cursor, keys = redis.zscan("location:#{location_avid}:uuids", cursor, match: "device:#{avid}:*", count: 1000)
    #   items.concat(keys.map(&:first))
    #   break if cursor.to_i == 0
    # end
    redis.hmget "location:#{location_avid}:changes", items unless items.empty?
  end

  def store_snapshot
    redis.zadd "location:#{location_avid}:snapshot", Time.now.to_i, MessagePack.pack(devices)
  end

  def latest_snapshot
    latest_snapshot = redis.zrange "location:#{location_avid}:snapshot", -1, -1
    MessagePack.unpack(latest_snapshot.last) unless latest_snapshot.empty?
  end

  def memory_usage
    bytes_to_megabytes(@redis.info["used_memory"].to_i - @initial_memory)
  end
end

sleep 5

Benchmark.bm do |benchmark|

  changes_v2 = ChangesV2.new(1, MsgPackSerializer)

  q = 50000

  create_time = benchmark.report "create" do
    changes_v2.create_changes(q)
  end

  puts "create/ms: #{(create_time.real / q * 3) * 1000}ms"

  merge_time = benchmark.report "merge" do
    changes_v2.fetch_and_merge
  end

  puts "merge/ms: #{(merge_time.real / q * 3) * 1000}ms"

  benchmark.report "find" do
    puts changes_v2.find(32805).size == 3
  end

  benchmark.report "create snapshot" do
    changes_v2.store_snapshot
  end

  benchmark.report "return snapshot" do
    changes_v2.latest_snapshot
  end

  puts "\nCount devices after merge: #{changes_v2.devices.size}. Memory: #{changes_v2.memory_usage}"
end
