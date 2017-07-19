local uv = require('luv')
local msgpack = require('MessagePack')
local uuid = require('uuid')

local logger = require('logger')
local config = require('config')
local utils = require('utils')

local file_logger = logger.new_file_sink(
  config.LOG_PATH, "MAIN", config.LOG_FLUSH_INTERVAL)

logger.prefix = "MAIN"
logger.sink = function(...)
  logger.console_sink(...)
  file_logger(...)
end

local shutdown_workers = false
local workers = {}

local worker_impl = string.dump(loadfile("worker.lua"))
local setup_worker

local function choose_worker()
  local worker_counts = {}

  for i, worker in ipairs(workers) do
    worker_counts[i] = {
      id = i,
      count = worker.conn_count,
    }
  end

  table.sort(worker_counts,
    function(lhs, rhs)
      return lhs.count < rhs.count
    end
  )

  return workers[worker_counts[1].id]
end

local function on_worker_close(worker_id, exit_status, term_signal)
  logger.info("on_worker_close", { worker_id = worker_id, exit_status = exit_status, term_signal = term_signal })

  local worker = workers[worker_id]

  if worker.pipe_to ~= nil then
    uv.close(worker.pipe_to)
    worker.pipe_to = nil
  end

  if worker.pipe_from ~= nil then
    uv.close(worker.pipe_from)
    worker.pipe_from = nil
  end

  worker.pid = nil
  worker.handle = nil

  if shutdown_workers then
    local worker_count = 0

    for i, worker in ipairs(workers) do
      if worker.handle ~= nil then
        worker_count = worker_count + 1
      end
    end

    if worker_count == 0 then
      logger.info("all workers closed")
      uv.stop()
    end
  else
    -- recover worker process
    local timer = uv.new_timer()

    uv.timer_start(timer, 1000, 0,
      function()
        uv.close(timer)
        setup_worker(uv.exepath(), worker_id)
      end
    )

    logger.info("respawn worker", worker_id, "in 1sec")
  end
end

local function on_worker_read(worker, err, data)
  if err then
  	logger.error(string.format("on_worker_read[W%03d] error", worker.id), err)
  	return
  end

  if uv.pipe_pending_count(worker.pipe_from) > 0 then
    local pending_type = uv.pipe_pending_type(worker.pipe_from)

    if pending_type == "tcp" then
      local client = uv.new_tcp()

      if uv.accept(worker.pipe_from, client) then
        local from = uv.tcp_getsockname(client)
        local to = uv.tcp_getpeername(client)
        local worker = choose_worker()
  
        logger.info("on_worker_read accepted", client, from, to)
        logger.info("on_worker_read transferring to",
          { worker_pid = worker.pid, worker_id = worker.id }
        )
        
        if not uv.write2(worker.pipe_to, data, client) then
          logger.error("on_worker_read failed to send client to",
            worker, msgpack.unpack(data)
          )
          uv.shutdown(client)
          uv.close(client)
        end 
      else
        logger.error(string.format("on_worker_read[W%03d] tcp accept fail", worker.id), client)
        uv.close(client)
      end
    else
      logger.error(string.format("on_worker_read[W%03d] cannot process pending_type", worker.id), pending_type)
      uv.stop()
    end

    return
  end
  
  if data then
    local unpacked = msgpack.unpack(data)

  	logger.debug(string.format("on_worker_read[W%03d] received data", worker.id), unpacked)

    if unpacked.type == "worker_stat" then
      worker.conn_count = unpacked.conn_count
    else
      logger.error(string.format("on_worker_read[W%03d] type not handled:", worker.id), unpacked.type)
    end

    return
  end

  if worker.shutting_down then
  	logger.debug(string.format("on_worker_read[W%03d] pipe closed", worker.id))
  else
  	logger.error(string.format("on_worker_read[W%03d] pipe closed", worker.id))
  end
end

function setup_worker(lua_path, worker_id)
  local pipe_to = uv.new_pipe(true)
  local pipe_from = uv.new_pipe(true)
  local input = uv.new_pipe(false)
  local handle, pid = uv.spawn(
    lua_path,
    {
      args = { "-", worker_id, "option2", },
      stdio = { input, 1, 2, pipe_to, pipe_from },
      env = { string.format("WORKER_ID=%d", worker_id) },
    }, 
    function(exit_status, term_signal)
      on_worker_close(worker_id, exit_status, term_signal)
    end
  )
  local worker = {
    id = worker_id,
    conn_count = 0,
  }

  workers[worker_id] = worker

  if pid == nil then
    -- spawn fail
    uv.close(pipe_to)
    uv.close(pipe_from)
    logger.error("worker spawn fail", worker)
  else
    -- spawn success
    worker.handle = handle
    worker.pid = pid
    worker.pipe_to = pipe_to
    worker.pipe_from = pipe_from
    uv.write(input, worker_impl)
    uv.read_start(worker.pipe_from,
      function(err, data)
        on_worker_read(worker, err, data)
      end
    )
    logger.info("worker spawn success", { worker_pid = worker.pid, worker_id = worker.id, })
  end

  uv.shutdown(input)
end

local function setup_workers()
  local lua_path = uv.exepath()
  local cpu_info = uv.cpu_info()

  logger.debug("lua_path", lua_path)

  for i = 1, #cpu_info do
    setup_worker(lua_path, i)
  end
end

local function new_session_info()
  return {
    session_id = uuid(),
  }
end

logger.info("setting up workers")
setup_workers()

local server = uv.new_tcp()

local function on_connect(err)
  assert(not err, err)

  local client = uv.new_tcp()
  local worker = choose_worker()
  local session_info = new_session_info()

  uv.accept(server, client)

  logger.info("on_connect transferring to chosen worker", worker, session_info)

  if not uv.write2(worker.pipe_to, msgpack.pack(session_info), client) then
    logger.error("on_connect failed to send client over pipe to", worker)
    uv.shutdown(client)
    uv.close(client)
  end 
end

logger.debug("installing sigint handler")
local sigint = uv.new_signal()

uv.signal_start(sigint, "sigint",
  function(signal)
    logger.info("signal handler with", signal, ", shutdown workers...")

    local shutdown_msg = msgpack.pack(
      { type = "shutdown", }
    )

    shutdown_workers = true

    for i, worker in ipairs(workers) do
      if worker.handle ~= nil then
        logger.debug("sending sigterm to", worker)
        worker.shutting_down = true
        uv.write(worker.pipe_to, shutdown_msg)
      end
    end
  end
)

do
  local t = uv.new_timer()

  uv.timer_start(t, 0, 0,
    function()
      logger.debug("setting up server socket", server)
      
      if uv.tcp_bind(server, "0.0.0.0", config.SERVER_PORT) then
        logger.info("server socket bound, listening...", server, uv.tcp_getsockname(server))
        if not uv.listen(server, 128, on_connect) then
          logger.error("failed to listen server socket", config.SERVER_PORT)
        end
  
        return
      else
        logger.error("failed to bind server socket", config.SERVER_PORT)
      end
    end
  )
end

logger.info("start event loop")
uv.run()
logger.info("done event loop")
logger.flush()
uv.loop_close()
