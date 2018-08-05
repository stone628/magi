local uv = require('luv')
local msgpack = require('MessagePack')
local floor = math.floor
local strlen = string.len
local strchar = string.char
local strbyte = string.byte
local strsub = string.sub

local function _encode_pipe_data(logger, type, data)
  local packed_data = msgpack.pack({ k = type, d = data, })
  local packed_data_len = strlen(packed_data)
  local encoded_data_len

  if packed_data_len < 0x80 then
    encoded_data_len = strchar(packed_data_len + 0x80)
  elseif packed_data_len < 0x4000 then
    encoded_data_len = strchar(
      floor(packed_data_len / 0x80),
      (packed_data_len % 0x80) + 0x80
    )
  elseif packed_data_len < 0x200000 then
    encoded_data_len = strchar(
      floor(packed_data_len / 0x4000),
      floor((packed_data_len % 0x4000) / 0x80),
      (packed_data_len % 0x80) + 0x80
    )
  else
    error(
      string.format("packed data too long(%d):%s", packed_data_len, data)
    )
  end

  return { [1] = encoded_data_len, [2] = packed_data }
end

local function _iterate_pipe_data(logger, data, pred)
  local data_len = strlen(data)
  local iter_start = 1
  local data_pos = 1
  local packed_len = 0

  while data_pos <= data_len do
    local d = strbyte(data, data_pos)

    if d >= 0x80 then
      packed_len = packed_len * 0x80 + d - 0x80

      local packed_data = strsub(data, data_pos + 1, data_pos + packed_len)
      local success, unpacked = pcall(msgpack.unpack, packed_data)

      if not success then
        logger.error(
          debug.traceback(
            string.format("%s: failed to unpack data", unpacked),
            2
          ),
          {
            raw_data = data,
            packed_start = data_pos + 1,
            packed_len = packed_len,
            packed_data = packed_data,
          }
        )
        return false
      end

      data_pos = data_pos + packed_len + 1
      pred(
        unpacked.k, unpacked.d,
        strsub(data, iter_start, data_pos - 1)
      )
      iter_start = data_pos
      packed_len = 0

      if data_pos > data_len then return true end
    else
      packed_len = packed_len * 0x80 + d
      data_pos = data_pos + 1
    end
  end
end

return setmetatable(
  {},
  {
    __call = function(_, logger)
      return {
        encode_pipe_data = function(type, data)
          return _encode_pipe_data(logger, type, data)
        end,
        iterate_pipe_data = function(data, pred)
          return _iterate_pipe_data(logger, data, pred)
        end,
      }
    end
  }
)
