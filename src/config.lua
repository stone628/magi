local uv = require('luv')

local config = {
  -- log related
  LOG_PATH = "./logs",
  LOG_FLUSH_INTERVAL = 1000,
  
  SERVER_PORT = 50000,
  SERVER_LOG_LEVEL = "debug",
}

return setmetatable({},
  {
    __index = config,
    __newindex = function(table, key, value)
      error("cannot modify config table")
    end,
    __metatable = false,
  }
)
