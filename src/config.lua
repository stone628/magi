local uv = require('luv')
local config = {}

-- log related
config.LOG_PATH = "./logs"
config.LOG_FLUSH_INTERVAL = 1000

uv.fs_mkdir(config.LOG_PATH, "644",
  function(err)
    if err then
      local errstr = tostring(err)

      if string.find(errstr, "EEXIST") ~= 1 then
        error(
          string.format("failed to prepare log path \"%s\", error:\"%s\"",
            config.LOG_PATH, errstr
          )
        )
      end
    end
  end
)

return setmetatable({},
  {
    __index = config,
    __newindex = function(table, key, value)
      error("cannot modify config table")
    end,
    __metatable = false,
  }
)
