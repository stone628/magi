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

local function on_client(err, data)
  logger.info("on_client enter", { err = err, data = data })

  if err then
    return
  end

  if uv.pipe_pending_count(queue) > 0 then
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
    else
      logger.info("on_client accept fail", client)
      uv.close(client)
    end
  elseif data then
    logger.info("on_client data", data)
  else
    logger.info("on_client detect end on queue")
    uv.stop()
  end
end

if uv.pipe_open(queue, 3) then
  uv.read_start(queue, on_client)
else
  uv.close(queue)
  uv.stop()
end

uv.run()
