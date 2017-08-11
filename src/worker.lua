local uv = require('luv')
local uuid = require('uuid')

local config = require('config')
local logger = require('logger')
local content = require('content')
local sutil = require('sutil')(logger)

local args = { ... }
local worker_id = tonumber(args[1])
local file_logger = logger.new_file_sink(
  config.LOG_PATH, string.format("W%03d", worker_id),
  config.LOG_FLUSH_INTERVAL)

logger.prefix = string.format("W%03d", worker_id)
logger.sink = function(...)
  logger.console_sink(...)
  file_logger(...)
end
logger.level = config.SERVER_LOG_LEVEL

local conn_count = 0
local sessions = {}

local from_server_pipe = uv.new_pipe(true)
local to_server_pipe = uv.new_pipe(true)

local sync_stat_count = 0
local sync_stat = uv.new_async(
  function()
    local worker_stat = {
      conn_count = conn_count,
      stat_count = tostring(sync_stat_count),
    }

    sync_stat_count = sync_stat_count + 1
    uv.write(to_server_pipe,
      sutil.encode_pipe_data("worker_stat",  worker_stat)
    )
  end
)

local function session_error(session, tag, err)
  logger.error(
    debug.traceback(
      string.format("%s:session(%s)", tag, session.session_id),
      3
    )
  )
end

local function session_write(session, data)
  if not session.valid then return end

  uv.write(session.connection, data)
end

local function session_from(session)
  return uv.tcp_getsockname(session.connection)
end

local function session_to(session)
  return uv.tcp_getpeername(session.connection)
end

local function session_close(session)
  if not session.valid then return end
  
  if session.on_close then
    xpcall(
      function() session.on_close(session) end,
      function(err) session_error(session, "on_close", err) end
    )
  end

  local conn = session.connection

  conn_count = conn_count - 1
  sessions[session.worker_session_id] = nil
  logger.debug("disconnecting session", { session_id = session.session_id })
  uv.read_stop(conn)
  uv.shutdown(conn, function() uv.close(conn) end)
  uv.async_send(sync_stat)
end

local function session_transfer(session)
  if not session.valid then return end

  local conn = session.connection
  local trans_data = sutil.encode_pipe_data(
    "transfer",
    {
      session_id = session.session_id,
      data = session.data,
      stat_count = tostring(sync_stat_count),
    }
  )

  if session.on_transfer then
    xpcall(
      function() session.on_transfer(session) end,
      function(err) session_error(session, "on_transfer", err) end
    )
  end

  uv.read_stop(conn)
 
  if not uv.write2(to_server_pipe, trans_data, conn) then
    logger.error("failed to transfer client to main", { session_id = session.session_id, data = session.data })
    uv.shutdown(conn, function() uv.close(conn) end)
  end

  session.valid = false
  conn_count = conn_count - 1
  sessions[session.worker_session_id] = nil
  uv.async_send(sync_stat)
end

local function session_create(session_info, conn, transferred)
  local session = {
    worker_session_id = uuid(),
    connection = conn,
    valid = true,
    write = session_write,
    close = session_close,
    transfer = session_transfer,
    from = session_from,
    to = session_to,
  }

  for k, v in pairs(session_info) do session[k] = v end

  sessions[session.worker_session_id] = session
  conn_count = conn_count + 1
  content.register_session_handlers(session)

  if session.on_connect then
    xpcall(
      function() session.on_connect(session, transferred) end,
      function(err) session_error(session, "on_connect", err) end
    )
  end

  uv.read_start(conn,
    function(err, data)
      logger.debug("session callback entered",
        {
          session_id = session.session_id,
          worker_session_id = session.worker_session_id,
          valid = session.valid,
          err = err,
          data = data
        }
      )
    
      if err then return end
    
      if data then
        if session.on_data then
          xpcall(
            function() session.on_data(session, data) end,
            function(err) session_error(session, "on_data", err) end
          )
        end

        return
      end
      
      session_close(session)
      logger.info("session callback disconnected", { conn_count = conn_count })
    end
  )

  uv.async_send(sync_stat)
  return new_session
end

local function on_main_pipe_session(data, transferred)
  if uv.pipe_pending_count(from_server_pipe) > 0 then
    if uv.pipe_pending_type(from_server_pipe) == "tcp" then
      local conn = uv.new_tcp()

      if uv.accept(from_server_pipe, conn) then
        local from = uv.tcp_getsockname(conn)
        local to = uv.tcp_getpeername(conn)
        
        session_create(data, conn, transferred)
        return
      end

      logger.error("on_main_pipe_session tcp accept fail", { data = data, transferred = transferred })
      return
    end
  end

  logger.error("on_main_pipe_session no pending tcp handle", { data = data, transferred = transferred })
end

local function on_main_pipe_read(type, data, raw_data)
  logger.debug("on_main_pipe_read", { type = type, data = data, raw_data = raw_data })

  if type == "connect" then
    on_main_pipe_session(data, false)
  elseif type == "transfer" then
    on_main_pipe_session(data, true)
  elseif type == "shutdown" then
    logger.info("on_main_pipe_read received shutdown", data)
    uv.stop()
  else
    logger.error("on_main_pipe_read data not handled", { type = type, data = data, raw_data = raw_data })
  end
end

local function content_error(tag, err)
  logger.error(
    debug.traceback(string.format("%s:%s", tag, err), 3)
  )
end

--------------------------------------------------------------------------------
-- worker main logic
--------------------------------------------------------------------------------
logger.info("starting new worker")

uuid.seed()

uv.signal_start(uv.new_signal(), "sigint",
  function(signal)
    logger.info("signal handler ignores signal", signal)
  end
)

repeat
  logger.debug("opening server_pipe")

  if not uv.pipe_open(to_server_pipe, 4) then
    logger.error("failed to open to_server_pipe")
    break
  end

  logger.debug("opening client acceptor")

  if not uv.pipe_open(from_server_pipe, 3) then
    logger.error("failed to open client acceptor")
    break
  end

  local content_handlers = {}

  content.register_content_handlers(content_handlers)

  logger.info("start event loop")

  uv.read_start(from_server_pipe,
    function(err, data)
      logger.debug("from_server_pipe read_start enter", { err = err, data = data })
    
      if not err then
        if data then
          sutil.iterate_pipe_data(data, on_main_pipe_read)
          return
        end

        logger.info("from_server_pipe read_start detect end")
      else
        return
      end
    
      uv.read_stop(from_server_pipe)
      uv.shutdown(from_server_pipe, function() uv.close(from_server_pipe) end)
    end
  )

  if content_handlers.on_startup then
    xpcall(
      function() content_handlers.on_startup(worker_id, logger) end,
      function(err) content_error("on_content_startup", err) end
    )
  end

  uv.run()

  if content_handlers.on_shutdown then
    xpcall(
      function() content_handlers.on_shutdown() end,
      function(err) content_error("on_content_shutdown", err) end
    )
  end

  logger.info("end event loop")
until true

logger.shutdown()

uv.run("once")
uv.loop_close()
