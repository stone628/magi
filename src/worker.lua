local uv = require('luv')
local msgpack = require('MessagePack')
local uuid = require('uuid')

local config = require('config')
local logger = require('logger')

local worker_id = tonumber(arg[1])
local file_logger = logger.new_file_sink(
  config.LOG_PATH, string.format("WORK%02d", worker_id),
  config.LOG_FLUSH_INTERVAL)

logger.prefix = string.format("[WORKER%02d]", worker_id)
logger.sink = function(...)
  logger.console_sink(...)
  file_logger(...)
end

local conn_count = 0
local sessions = {}

local queue = uv.new_pipe(true)
local server_pipe = uv.new_pipe(false)

local function worker_stat()
  return {
    type = "worker_stat",
    conn_count = conn_count,
  }
end

local function session_write(session, data)
  uv.write(session.connection, data)
end

local function session_close(session)
  conn_count = conn_count - 1
  sessions[session.worker_session_id] = nil
  uv.close(session.connection)
  uv.write(server_pipe, msgpack.pack(worker_stat()))
end

local function on_client_read(session, err, data)
  logger.info("on_client_read", session, { err = err, data = data })

  if err then return end

  if data then
    logger.info("on_client_read received", data)
    session_write(session, data)
    return
  end
  
  session_close(session)
  logger.info("on_client_read disconnected", { conn_count = conn_count })
end

local function session_create(session_info, conn)
  local new_session = {
    session_id = session_info.session_id,
    worker_session_id = uuid(),
    connection = conn,
  }

  sessions[new_session.worker_session_id] = new_session
  conn_count = conn_count + 1
  uv.read_start(conn,
    function(err, data)
      on_client_read(new_session, err, data)
    end
  )
  uv.write(server_pipe, msgpack.pack(worker_stat()))
  return new_session
end

local function on_client(err, data)
  logger.info("on_client enter", { err = err, data = data })

  if err then return end

  if uv.pipe_pending_count(queue) > 0 then 
    local pending_type = uv.pipe_pending_type(queue)

    logger.info("on_client pending type", pending_type)

    if pending_type == "tcp" then
      local conn = uv.new_tcp()

      if uv.accept(queue, conn) then
        local from = uv.tcp_getsockname(conn)
        local to = uv.tcp_getpeername(conn)
        local new_session = session_create(msgpack.unpack(data), conn)

        logger.info("on_client accepted", conn,
          { from = from, to = to, conn_count = conn_count}
        )
        session_write(new_session,
          string.format("Hello conn #%d from %s:%d to %s:%d in worker %d!\n",
            conn_count, from.ip, from.port, to.ip, to.port, worker_id
          )
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

logger.info("starting new worker", worker_id, "option", arg)

repeat
  logger.info("opening server_pipe")

  if not uv.pipe_open(server_pipe, 4) then
    logger.error("failed to open server_pipe")
    break
  end
  
  logger.info("opening client acceptor")

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
