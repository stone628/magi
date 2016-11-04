local uv = require('luv')
local utils = require('utils')
local logger = {}

logger.level = "info"
logger.prefix = ""

local modes = {
  { name = "trace", color = "blue", },
  { name = "debug", color = "cyan", },
  { name = "info", color = "green", },
  { name = "warn", color = "yellow", },
  { name = "error", color = "red", },
  { name = "fatal", color = "magenta", },
}

local levels = {}

local function pretty_string(...)
  local n = select('#', ...)
  local arguments = { ... }

  for i = 1, n do
    arguments[i] = utils.dump(arguments[i])
  end

  return table.concat(arguments, "\t")
end

for i, v in ipairs(modes) do
  local header_color = v.color
  local upper_name = v.name:upper()

  levels[v.name] = i
  logger[v.name] = function(...)
    if logger.sink == nil or i < levels[logger.level] then return end

    local msg = pretty_string(...)
    local info = debug.getinfo(2, "Sl")

    logger.sink(
      utils.colorize(header_color,
        string.format("[%-6s%s %s:%s]",
          upper_name, os.date("%H:%M:%S"), info.short_src, info.currentline
        )
      ) .. string.format(" %s%s\n", logger.prefix, msg)
    )
  end
end

function logger.console_sink(message)
  uv.write(utils.stdout, message)
end

function logger.new_file_sink(path)
  local timer = uv.new_timer()
  local context = {
    logs = {},
    file = uv.fs_open(path, "a+", "rw",
      function(err)
        if err then
          logger.error("failed to open log file", path)
          context.file = nil
        end
      end
    )
  }

  timer:start(5, true,
    function()
      if context.file == nil then
        logger.error("open log file not opened", path)
      else
        if table.maxn(context.logs) > 0 then
          uv.write(context.file, table.concat(context.logs))
          context.logs = {}
        end
      end
    end
  )

  return function(message)
    if context.file ~= nil then
      -- strip ASCII color codes
      table.insert(context.logs, message)
    end
  end
end

logger.sink = logger.console_sink

return logger
