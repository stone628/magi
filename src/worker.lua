local uv = require('luv')
local msgpack = require('MessagePack')
local uuid = require('uuid')

local config = require('config')
local logger = require('logger')
local content = require('content')

local worker_id = tonumber(arg[1])
local file_logger = logger.new_file_sink(
  config.LOG_PATH, string.format("W%03d", worker_id),
  config.LOG_FLUSH_INTERVAL)

logger.prefix = string.format("W%03d", worker_id)
logger.sink = function(...)
  logger.console_sink(...)
  file_logger(...)
end

local conn_count = 0
local sessions = {}

local queue = uv.new_pipe(true)
local server_pipe = uv.new_pipe(true)

local function worker_stat()
  return {
    type = "worker_stat",
    conn_count = conn_count,
  }
end

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

  conn_count = conn_count - 1
  sessions[session.worker_session_id] = nil
  uv.close(session.connection)
  uv.write(server_pipe, msgpack.pack(worker_stat()))
end

local function session_transfer(session)
  if not session.valid then return end

  if session.on_transfer then
    xpcall(
      function() session.on_transfer(session) end,
      function(err) session_error(session, "on_transfer", err) end
    )
  end

  local conn = session.connection
  local trans_data = {
    session_id = session.session_id,
  }
  uv.read_stop(conn)

  if not uv.write2(server_pipe, msgpack.pack(trans_data), conn) then
    logger.error("failed to transfer client",
      { session_id = session.session_id, }
    )
    uv.shutdown(conn)
    uv.close(conn)
  end

  session.valid = false
  sessions[session.worker_session_id] = nil
  uv.write(server_pipe, msgpack.pack(worker_stat()))
end

local function session_create(session_info, conn)
  local session = {
    session_id = session_info.session_id,
    worker_session_id = uuid(),
    connection = conn,
    valid = true,
    write = session_write,
    close = session_close,
    transfer = session_transfer,
    from = session_from,
    to = session_to,
  }

  sessions[session.worker_session_id] = session
  conn_count = conn_count + 1
  content(session)

  if session.on_connect then
    xpcall(
      function() session.on_connect(session) end,
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

  uv.write(server_pipe, msgpack.pack(worker_stat()))
  return new_session
end

local function on_client(err, data)
  logger.debug("on_client enter", { err = err, data = data })

  if err then return end

  if uv.pipe_pending_count(queue) > 0 then 
    local pending_type = uv.pipe_pending_type(queue)

    logger.debug("on_client pending type", pending_type)

    if pending_type == "tcp" then
      local conn = uv.new_tcp()

      if uv.accept(queue, conn) then
        local from = uv.tcp_getsockname(conn)
        local to = uv.tcp_getpeername(conn)
        local new_session = session_create(msgpack.unpack(data), conn)

        logger.info("on_client accepted", conn,
          { from = from, to = to, conn_count = conn_count}
        )
      else
        logger.error("on_client tcp accept fail", conn)
        uv.close(conn)
      end
    else
      logger.error("on_client cannot process pending_type", pending_type)
      uv.stop()
    end

    return
  end

  if data then
    logger.info("on_client data", msgpack.unpack(data))
    return
  end

  logger.info("on_client detect end on queue")
  uv.stop()
end

logger.info("starting new worker")

repeat
  logger.debug("opening server_pipe")

  if not uv.pipe_open(server_pipe, 4) then
    logger.error("failed to open server_pipe")
    break
  end
  
  logger.debug("opening client acceptor")

  if not uv.pipe_open(queue, 3) then
    logger.error("failed to open client acceptor")
    break
  end

  logger.info("start event loop")
  uv.read_start(queue, on_client)
  uv.run()
  logger.info("end event loop")
until true

uv.close(queue)
uv.close(server_pipe)
