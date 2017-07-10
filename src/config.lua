local uv = require('luv')

local config = {
  -- log related
  LOG_PATH = "./logs",
  LOG_FLUSH_INTERVAL = 1000,
  
  SERVER_PORT = 50000,
}

uv.fs_mkdir(config.LOG_PATH, "755",
  function(err)
    if err then
      local errstr = tostring(err)

      if string.find(errstr, "EEXIST") ~= 1 then
        error(
          string.format(
            "failed to prepare log path \"%s\", error:\"%s\"",
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
