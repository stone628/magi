local uv = require('luv')
local SERVER_VERSION = 0.1

local function check_defaults(content_config, logger)
  -- log related
  content_config.LOG_PATH = content_config.LOG_PATH or "logs"
  content_config.LOG_FLUSH_INTERVAL = content_config.LOG_FLUSH_INTERVAL or 1000

  -- server related
  if content_config.SERVER_PORT == nil then
    logger.error("missing SERVER_PORT in magi_config")
    return nil
  end

  content_config.SERVER_WORKER_COUNT = content_config.SERVER_WORKER_COUNT or #uv.cpu_info()
  content_config.SERVER_LOG_LEVEL = content_config.SERVER_LOG_LEVEL or "info"

  content_config.VERSION = SERVER_VERSION

  return setmetatable(
    {},
    {
      __index = content_config,
      __newindex = function(table, key, value)
        logger.error("cannot modify config table")
      end,
      __metatable = false,
    }
  )
end

return setmetatable(
  {},
  {
    __call = function(_, content_config, logger)
      return check_defaults(content_config, logger)
    end
  }
)
