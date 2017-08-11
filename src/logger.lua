local uv = require('luv')
-- code from https://github.com/luvit/luv/blob/master/lib/utils.lua
local utils = {}
local shutdown_signal = {}
local usecolors

local colors = {
  black   = "0;30",
  red     = "0;31",
  green   = "0;32",
  yellow  = "0;33",
  blue    = "0;34",
  magenta = "0;35",
  cyan    = "0;36",
  white   = "0;37",
  B        = "1;",
  Bblack   = "1;30",
  Bred     = "1;31",
  Bgreen   = "1;32",
  Byellow  = "1;33",
  Bblue    = "1;34",
  Bmagenta = "1;35",
  Bcyan    = "1;36",
  Bwhite   = "1;37"
}

function utils.color(color_name)
  if usecolors then
    return "\27[" .. (colors[color_name] or "0") .. "m"
  else
    return ""
  end
end

function utils.colorize(color_name, string, reset_name)
  return utils.color(color_name) .. tostring(string) .. utils.color(reset_name)
end

local backslash, null, newline, carriage, tab, quote, quote2, obracket, cbracket

function utils.loadColors(n)
  if n ~= nil then usecolors = n end
  backslash = utils.colorize("Bgreen", "\\\\", "green")
  null      = utils.colorize("Bgreen", "\\0", "green")
  newline   = utils.colorize("Bgreen", "\\n", "green")
  carriage  = utils.colorize("Bgreen", "\\r", "green")
  tab       = utils.colorize("Bgreen", "\\t", "green")
  quote     = utils.colorize("Bgreen", '"', "green")
  quote2    = utils.colorize("Bgreen", '"')
  obracket  = utils.colorize("B", '[')
  cbracket  = utils.colorize("B", ']')
end

function utils.dump(o, depth)
  local t = type(o)
  if t == 'string' then
    return quote .. o:gsub("\\", backslash):gsub("%z", null):gsub("\n", newline):gsub("\r", carriage):gsub("\t", tab) .. quote2
  end
  if t == 'nil' then
    return utils.colorize("Bblack", "nil")
  end
  if t == 'boolean' then
    return utils.colorize("yellow", tostring(o))
  end
  if t == 'number' then
    return utils.colorize("blue", tostring(o))
  end
  if t == 'userdata' then
    return utils.colorize("magenta", tostring(o))
  end
  if t == 'thread' then
    return utils.colorize("Bred", tostring(o))
  end
  if t == 'function' then
    return utils.colorize("cyan", tostring(o))
  end
  if t == 'cdata' then
    return utils.colorize("Bmagenta", tostring(o))
  end
  if t == 'table' then
    if type(depth) == 'nil' then
      depth = 0
    end
    if depth > 1 then
      return utils.colorize("yellow", tostring(o))
    end
    local indent = ("  "):rep(depth)

    -- Check to see if this is an array
    local is_array = true
    local i = 1
    for k,v in pairs(o) do
      if not (k == i) then
        is_array = false
      end
      i = i + 1
    end

    local first = true
    local lines = {}
    i = 1
    local estimated = 0
    for k,v in (is_array and ipairs or pairs)(o) do
      local s
      if is_array then
        s = ""
      else
        if type(k) == "string" and k:find("^[%a_][%a%d_]*$") then
          s = k .. ' = '
        else
          s = '[' .. utils.dump(k, 100) .. '] = '
        end
      end
      s = s .. utils.dump(v, depth + 1)
      lines[i] = s
      estimated = estimated + #s
      i = i + 1
    end
    if estimated > 200 then
      return "{\n  " .. indent .. table.concat(lines, ",\n  " .. indent) .. "\n" .. indent .. "}"
    else
      return "{ " .. table.concat(lines, ", ") .. " }"
    end
  end
  -- This doesn't happen right?
  return tostring(o)
end

-- A nice global data dumper
function utils.pretty_string(...)
  local n = select('#', ...)
  local arguments = { ... }

  for i = 1, n do
    arguments[i] = utils.dump(arguments[i])
  end

  return table.concat(arguments, "\t")
end

function utils.initialize()
  if utils.stdout then return end

  if uv.guess_handle(1) == "tty" then
    utils.stdout = uv.new_tty(1, false)
    usecolors = true
  else
    utils.stdout = uv.new_pipe(false)
    uv.pipe_open(utils.stdout, 1)
    usecolors = false
  end
  
  utils.loadColors()
end

function utils.write(message)
  uv.write(utils.stdout, message)
end

function utils.finalize()
  if utils.stdout then
    uv.close(utils.stdout)
    utils.stdout = nil
  end
end

local logger = {}

logger.level = "info"
logger.prefix = ""

local modes = {
  { name = "trace", color = "blue", short = "TRACE", },
  { name = "debug", color = "cyan", short = "DEBUG", },
  { name = "info", color = "green", short = "INFO ", },
  { name = "warn", color = "yellow", short = "WARN ", },
  { name = "error", color = "red", short = "ERROR", },
  { name = "fatal", color = "magenta", short = "FATAL" },
}

local levels = {}

for i, v in ipairs(modes) do
  local header_color = v.color
  local upper_name = v.short

  levels[v.name] = i
  logger[v.name] = function(msg, ...)
    if logger.sink == nil or i < levels[logger.level] then return end

    local rest_msg = utils.pretty_string(...)
    local info = debug.getinfo(2, "Sl")

    logger.sink(
      utils.colorize(header_color,
        string.format("[%s %s %s@%s:%s]%s",
          upper_name, os.date("%H:%M:%S"), logger.prefix,
          info.short_src, info.currentline, msg
        )
      ) .. string.format("\t%s\n", rest_msg)
    )
  end
end

function logger.console_sink(message)
  if message == shutdown_signal then
    utils.finalize()
  elseif message then
    utils.initialize()
    utils.write(message)
  end
end

local contexts = {}
local FILE_SINK_FLUSH_INTERVAL = 5000

function logger.new_file_sink(path, name, interval)
  local context = {
    logs = {},
    modified = false,
    timer = nil,
  }
  local on_timer
  local function clear_context()
    if context.modified then
      local logs = context.logs

      for i, _ in ipairs(logs) do logs[i] = nil end
      context.modified = false
    end
  end
  local function flush_context()
    if context.modified then
      if context.file ~= nil then
        local log_data = table.concat(context.logs)

        uv.fs_write(context.file, log_data, -1,
          function(err, chunk)
            if err then
              logger.error("failed to write to log file", context.filename, err)
              uv.fs_close(context.file)
              context.file = nil
            end
          end
        )
      end

      clear_context()
    end
  end

  interval = interval or FILE_SINK_FLUSH_INTERVAL

  on_timer = function()
    local date_prefix = os.date("%y%m%d")

    if context.date_prefix == date_prefix then
      if context.file == nil then
        clear_context()
        return
      end

      flush_context()
    else
      if context.file ~= nil then
        uv.fs_close(context.file)
        context.file = nil
      end

      context.date_prefix = date_prefix
      context.filename = string.format("%s/%s_%s%s.log",
        path, name, date_prefix, os.date("%H%M%S"))

      uv.fs_open(
        context.filename, "a+", tonumber("644", 8),
        function(err, fd)
          if err then
            logger.error("failed to open log file", context.filename, err)
            context.file = nil
            clear_context()
          else
            context.file = fd
            flush_context()

            if not context.timer then
              local timer = uv.new_timer()
        
              context.timer = timer
              uv.timer_start(timer, interval, interval, on_timer)
            end
          end
        end
      )
    end
  end

  table.insert(contexts, context)

  uv.fs_mkdir(path, tonumber("755", 8),
    function(err)
      if err then
        local errstr = tostring(err)
  
        if string.find(errstr, "EEXIST") ~= 1 then
          error(
            string.format(
              "failed to prepare log path \"%s\", error:\"%s\"",
              path, errstr
            )
          )
          return
        end
      end

      on_timer()
    end
  )

  return function(message)
    if message == shutdown_signal then
      on_timer()

      if context.timer then
        uv.timer_stop(context.timer)
        uv.close(context.timer)
        context.timer = nil
      end
    elseif message then
      -- strip ASCII color codes
      -- following pattern cannot cover exact ASCII color codes
      -- because of lua pattern limitation
      local stripped = string.gsub(message, "\027%[([0-9;]+)m", "")

      table.insert(context.logs, stripped)
      context.modified = true
    else
      on_timer()
    end
  end
end

logger.sink = logger.console_sink
logger.flush = function()
  logger.sink(nil)
end
logger.shutdown = function()
  logger.sink(shutdown_signal)
end

return logger
