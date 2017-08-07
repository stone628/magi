local cutil
local logger
local wid

local gc_timer

local function collect_and_report()
  local mem_before = string.format("%dKB", collectgarbage("count"))

  collectgarbage()

  local mem_after = string.format("%dKB", collectgarbage("count"))

  logger.info("garbage collected", { before = mem_before, after = mem_after })
end

local session_count = 0
local last_memory = 0

local function report()
  local mem_used = collectgarbage("count")

  logger.info("status report:",
    {
      ["memory used:"] = string.format("%dKB", mem_used),
      ["memory diff:"] = string.format("%dKB", mem_used - last_memory),
      ["session count:"] = session_count,
    }
  )
  last_memory = mem_used
end

local function on_content_startup(worker_id, worker_logger)
  wid = worker_id
  logger = worker_logger
  cutil = require("cutil")(logger)

  logger.info("on_content_startup", { worker_id = wid })

  gc_timer = cutil.set_interval(30000, report)
end

local function on_content_shutdown()
  logger.info("on_content_shutdown", { worker_id = wid })

  cutil.clear_interval(gc_timer)
end

local function on_session_connect(session, transferred)
  local from = session:from()
  local to = session:to()

  if transferred then
    session:write(
      string.format("transferred session %s:%s from %s:%d to %s:%d\n",
        session.session_id, session.worker_session_id,
        from.ip, from.port, to.ip, to.port
      )
    )
  else
    session:write(
      string.format("new session %s:%s from %s:%d to %s:%d\n",
        session.session_id, session.worker_session_id,
        from.ip, from.port, to.ip, to.port
      )
    )
  end

  session_count = session_count + 1
end

local function on_session_data(session, data)
  -- echo content
  if string.find(data, "transfer") == 1 then
    session:write("trying transfer\n")
    session:transfer()
  else
    session:write(data)
  end
end

local function on_session_close(session)
  session_count = session_count - 1
end

local function on_session_transfer(session)
  session_count = session_count - 1
end

return {
  register_content_handlers = function(content)
    content.on_startup = on_content_startup
    content.on_shutdown = on_content_shutdown
  end,
  register_session_handlers = function(session)
    session.on_connect = on_session_connect
    session.on_data = on_session_data
    session.on_close = on_session_close
    session.on_transfer = on_session_transfer
  end,
}
