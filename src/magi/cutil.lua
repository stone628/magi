local uv = require('luv')

local function log_error(logger, prefix, depth)
  logger.error(debug.traceback(prefix, depth))
end

-- timer related
local function set_timeout(logger, timeout, callback)
  local timer = uv.new_timer()

  uv.timer_start(timer, timeout, 0,
    function()
      uv.timer_stop(timer)
      uv.close(timer)
      xpcall(
        callback,
        function()
          log_error(
            logger,
            string.format("timer(%s:%d):", timer, timeout),
            3
          )
        end
      )
    end
  )

  return timer
end

local function set_interval(logger, interval, callback)
  local timer = uv.new_timer()

  uv.timer_start(timer, interval, interval,
    function()
      xpcall(
        callback,
        function()
          log_error(
            logger,
            string.format("interval(%s:%d):", timer, interval),
            3
          )
        end
      )
    end
  )

  return timer
end

local function clear_interval(timer)
  uv.timer_stop(timer)
  uv.close(timer)
end

-- unity style coroutine, with uv library
local cor_tasks = nil
local cor_to_task = {}
local cor_task_serial = 0

local function init_cor(logger)
  if cor_tasks then return end

  local cor_idle = uv.new_idle()

  cor_tasks = {}
  cor_task_serial = 0
  uv.idle_start(cor_idle,
    function()
      for i, task in ipairs(cor_tasks) do
        if coroutine.status(task.co) == "dead" then
        end
      end
    end
  )
end

local function cor_start(logger, func, ...)
  init_cor(logger)
  cor_task_serial = cor_task_serial + 1

  local task = {
    id = cor_task_serial,
    co = coroutine.create(func),
    yielded = nil,
  }

  table.insert(cor_tasks, task)
  cor_to_task[task.co] = task

  local result, returned = coroutine.resume(task.co, ...)

  if result then
    task.yielded = returned
  else
    logger.error(
      debug.traceback(
        string.format("cor_start(%d):%s", task.id, returned),
        1
      )
    )
    logger.info("coroutine", task.co, "became", coroutine.status(task.co))
  end

  return task.id
end

local function cor_stop(logger, cor)
end

local function cor_yield(logger, ...)
end

local function cor_break()
end

return function(logger)
  return {
    set_timeout = function(timeout, callback)
      return set_timeout(logger, timeout, callback)
    end,
    set_interval = function(interval, callback)
      return set_interval(logger, interval, callback)
    end,
    clear_interval = clear_interval,

    cor_start = function(func, ...)
      return cor_start(logger, func, ...)
    end,
    cor_stop = function(cor)
      return cor_stop(logger, cor)
    end,
    cor_break = cor_break,
    cor_yield = function(...)
      return cor_yield(logger, ...)
    end,
  }
end
