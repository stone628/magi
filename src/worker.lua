local worker_id = tonumber(arg[1])
local uv = require('luv')
local config = require('config')
local logger = require('logger')
local file_logger = logger.new_file_sink(
  config.LOG_PATH, string.format("WORK%02d", worker_id),
  config.LOG_FLUSH_INTERVAL)

logger.prefix = string.format("[WORKER%02d]", worker_id)
logger.sink = function(...)
  logger.console_sink(...)
  file_logger(...)
end

local conn_count = 0

logger.info("starting new worker", worker_id, "option", arg)

local queue = uv.new_pipe(true)
local server_pipe = uv.new_pipe(false)

local function on_client(err, data)
  logger.info("on_client enter", { err = err, data = data })

  if err then
    return
  end

  if uv.pipe_pending_count(queue) > 0 then 
    local pending_type = uv.pipe_pending_type(queue)

    logger.info("on_client pending type", pending_type)

    if pending_type == "tcp" then
      local client = uv.new_tcp()

      if uv.accept(queue, client) then
        local from = uv.tcp_getsockname(client)
        local to = uv.tcp_getpeername(client)
  
        logger.info("on_client accepted", client, from, to)
        uv.write(client,
          string.format("BYE connection #%d from %s:%d to %s:%d in worker %d!\n",
            conn_count, from.ip, from.port, to.ip, to.port, worker_id
          )
        )
        uv.shutdown(client,
          function()
            logger.info("on_client closing", client, from, to)
            uv.close(client)
          end
        )
        conn_count = conn_count + 1
        uv.write(server_pipe, conn_count)
      else
        logger.error("on_client tcp accept fail", client)
        uv.close(client)
      end
    else
      logger.error("on_client cannot process pending_type", pending_type)
      uv.stop()
    end

    return
  end

  if data then
    logger.info("on_client data", data)
    return
  end

  logger.info("on_client detect end on queue")
  uv.stop()
end

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
until false

uv.close(queue)
uv.close(server_pipe)
