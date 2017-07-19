local logger
local wid

local function on_content_startup(worker_id, worker_logger)
  wid = worker_id
  logger = worker_logger

  logger.info("on_content_startup", { worker_id = wid })
end

local function on_content_shutdown()
  logger.info("on_content_shutdown", { worker_id = wid })
end

local function on_session_connect(session)
  local from = session:from()
  local to = session:to()

  session:write(
    string.format("Hello session %s:%s from %s:%d to %s:%d\n",
      session.session_id, session.worker_session_id,
      from.ip, from.port, to.ip, to.port
    )
  )
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
end

local function on_session_transfer(session)
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