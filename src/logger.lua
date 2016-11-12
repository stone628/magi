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

local contexts = {}
local FILE_SINK_FLUSH_INTERVAL = 5000

function logger.new_file_sink(path, name, interval)
  local context = {
    logs = {},
  }
  local on_timer

  interval = interval or FILE_SINK_FLUSH_INTERVAL

  on_timer = function()
    local date_prefix = os.date("%y%m%d")

    --print("on_timer", utils.dump(context))

    if context.date_prefix == date_prefix then
      if context.opening == false and context.file == nil then
        return
      end
    else
      if context.file ~= nil then
        uv.fs_close(context.file)
        context.file = nil
      end

      context.date_prefix = date_prefix
      context.filename = string.format("%s/%s_%s%s.log",
        path, name, date_prefix, os.date("%H%M%S"))
      context.opening = true
      uv.fs_open(
        context.filename, "a+", tonumber("644", 8),
        function(err, fd)
          context.opening = false
      
          if err then
            logger.error("failed to open log file", context.filename, err)
            context.file = nil
          else
            local timer = uv.new_timer()
      
            context.file = fd
            on_timer()
            uv.timer_start(timer, interval, interval, on_timer)
          end
        end
      )
    end

    if table.maxn(context.logs) > 0 then
      if context.file ~= nil then
        local log_data = table.concat(context.logs)

        -- clear accum log
        for i in ipairs(context.logs) do context.logs[i] = nil end

        uv.fs_write(context.file, log_data, -1,
          function(err, chunk)
            if err then
              logger.error("failed to write to log file", context.filename, err)
            end
          end
        )
      end
    end
  end


  table.insert(contexts, context)
  on_timer()

  return function(message)
    if context.opening == true or context.file ~= nil then
      -- strip ASCII color codes
      -- following pattern cannot cover exact ASCII color codes
      -- because of lua pattern limitation
      local stripped = string.gsub(message, "\27%[[01][;%d]+m", "")

      table.insert(context.logs, stripped)
    end
  end
end

logger.sink = logger.console_sink

return logger
