local uv = require('luv')
local uuid = require('uuid')
local logger = require('magi.logger')
local sutil = require('magi.sutil')(logger)

local runner_path = { ... }

local shutdown_workers = false
local workers = {}
local worker_impl = string.dump(require('magi.worker'))
local setup_worker

logger.prefix = "MAIN"

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
  logger.info("on_worker_close",
    {
      worker_id = worker_id, exit_status = exit_status,
      term_signal = term_signal
    }
  )

  local worker = workers[worker_id]

  if worker.pipe_to then
    uv.close(worker.pipe_to)
    worker.pipe_to = nil
  end

  if worker.pipe_from then
    uv.close(worker.pipe_from)
    worker.pipe_from = nil
  end

  if worker.handle then
    uv.close(worker.handle)
    worker.handle = nil
  end

  worker.pid = nil

  if shutdown_workers then
    local worker_count = 0

    for i, worker in ipairs(workers) do
      if worker.handle then
        worker_count = worker_count + 1
      end
    end

    if worker_count == 0 then
      logger.info("all workers closed")
      uv.stop()
    end
  else
    local timer = uv.new_timer()
    -- recover worker process
    uv.timer_start(timer, 1000, 0,
      function()
        uv.timer_stop(timer)
        uv.close(timer)
        setup_worker(uv.exepath(), worker_id)
      end
    )

    logger.info("respawn worker", worker_id, "in 1sec")
  end
end

local function on_worker_pipe_read(worker, type, data, raw_data)
  if type == "worker_stat" then
    worker.conn_count = data.conn_count
  elseif type == "transfer" then
    local pipe_from = worker.pipe_from
    local transfer_success = false

    if uv.pipe_pending_count(pipe_from) > 0 then
      if uv.pipe_pending_type(pipe_from) == "tcp" then
        local client = uv.new_tcp()

        if uv.accept(worker.pipe_from, client) then
          local from = uv.tcp_getsockname(client)
          local to = uv.tcp_getpeername(client)
          local chosen_worker = choose_worker()

          logger.debug(
            string.format("on_worker_pipe_read[W%03d] accepted", worker.id),
            { from = from, to = to }
          )
          logger.info(
            string.format("on_worker_pipe_read[W%03d] transferring to", worker.id),
            { worker_pid = chosen_worker.pid, worker_id = chosen_worker.id, }
          )

          if uv.write2(chosen_worker.pipe_to, raw_data, client) then
            return
          end

          logger.error(
            string.format("on_worker_pipe_read[W%03d] failed to send client to", worker.id),
            { worker_id = chosen_worker.id, raw_data = raw_data}
          )
          uv.shutdown(client, function() uv.close(client) end)
        else
          logger.error(
            string.format("on_worker_pipe_read[W%03d] tcp accept fail", worker.id),
            client
          )
          uv.close(client)
        end

        return
      end
    end

    logger.error(
      string.format("on_worker_pipe_read[W%03d] no pending tcp handle", worker.id)
    )
    uv.stop()
  else
    logger.error(
      string.format("on_worker_pipe_read[W%03d] type not handled:", worker.id),
      type
    )
  end
end

local function on_worker_read(worker, err, data)
  if not err then
    if data then
      sutil.iterate_pipe_data(
        data,
        function(type, data, raw_data)
          on_worker_pipe_read(worker, type, data, raw_data)
        end
      )
      return true
    end

    if worker.shutting_down then
    	logger.debug(string.format("on_worker_read[W%03d] pipe closed", worker.id))
    else
    	logger.error(string.format("on_worker_read[W%03d] pipe closed", worker.id))
    end
  else
  	logger.error(string.format("on_worker_read[W%03d] error", worker.id), err)
  end

  return false
end

function setup_worker(lua_path, worker_id, content_path)
  local pipe_to = uv.new_pipe(true)
  local pipe_from = uv.new_pipe(true)
  local input = uv.new_pipe(false)
  local handle, pid = uv.spawn(
    lua_path,
    {
      args = { "-", worker_id, content_path, },
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
    uv.read_start(pipe_from,
      function(err, data)
        if on_worker_read(worker, err, data) then return end

        uv.read_stop(pipe_from)
        worker.pipe_from = nil
        worker.pipe_to = nil
        uv.shutdown(pipe_from, function() uv.close(pipe_from) end)
        uv.shutdown(pipe_to, function() uv.close(pipe_to) end)
      end
    )
    logger.info("worker spawn success",
      { worker_pid = worker.pid, worker_id = worker.id, }
    )
  end

  uv.shutdown(input)
end

local function setup_workers(config, content_path)
  local lua_path = uv.exepath()

  logger.debug("setting up workers, lua_path:", lua_path,
      "worker_count:", config.SERVER_WORKER_COUNT)

  for i = 1, config.SERVER_WORKER_COUNT do
    setup_worker(lua_path, i, content_path)
  end
end

local function new_session_info()
  return {
    session_id = uuid(),
  }
end

local function on_connect(server, err)
  if err then
    logger.error("on_connect error", err)
    return
  end

  local client = uv.new_tcp()
  local worker = choose_worker()
  local new_session = new_session_info()
  local session_data = sutil.encode_pipe_data("connect", new_session)

  uv.accept(server, client)

  logger.info("on_connect transferring to chosen worker",
    { worker_id = worker.id, pid = worker.pid, session_id = new_session.session_id }
  )

  if not uv.write2(worker.pipe_to, session_data, client) then
    logger.error("on_connect failed to send client over pipe to",
      { worker_id = worker.id, session_id = new_session.session_id }
    )

    uv.shutdown(client, function() uv.close(client) end)
  end
end

local function main(config, content_path)
  local file_logger = logger.new_file_sink(
    config.LOG_PATH, logger.prefix, config.LOG_FLUSH_INTERVAL)

  logger.level = config.SERVER_LOG_LEVEL
  logger.sink = function(...)
    logger.console_sink(...)
    file_logger(...)
  end

  logger.info("starting magi server, version", config.VERSION)

  uuid.seed()

  uv.signal_start(uv.new_signal(), "sigint",
    function(signal)
      local shutdown_msg = sutil.encode_pipe_data("shutdown", {})

      logger.info("signal handler with", signal, ", shutdown workers...")

      shutdown_workers = true

      for i, worker in ipairs(workers) do
        if worker.handle and worker.pipe_to then
          logger.debug("sending shutdown to",
            { worker_id = worker.id, worker_pid = worker.pid }
          )
          worker.shutting_down = true
          uv.write(worker.pipe_to, shutdown_msg)
        end
      end
    end
  )

  setup_workers(config, content_path)

  uv.timer_start(uv.new_timer(), 0, 0,
    function()
      local server = uv.new_tcp()

      logger.debug("setting up server socket")

      if uv.tcp_bind(server, "::", config.SERVER_PORT) then
        logger.info("server socket bound, listening...", server, uv.tcp_getsockname(server))

        local result = uv.listen(server, 128,
          function(err) on_connect(server, err) end
        )

        if not result then
          logger.error("failed to listen server socket", config.SERVER_PORT)
        end

        return
      end

      logger.error("failed to bind server socket", config.SERVER_PORT)
      uv.stop()
    end
  )

  logger.info("start event loop")
  uv.run()
  logger.info("done event loop")

  -- finalize
  logger.shutdown()
  uv.run("once")
  uv.loop_close()
  return 0
end

return setmetatable(
  {},
  {
    __call = function(_, content_path)
      local valid, config = pcall(
        function()
          return require('magi.config')(require('magi_config'), logger)
        end
      )

      if not valid then
        logger.error("magi runner startup failed", config)
        return 1
      end

      if not config then return 2 end

      return main(config, content_path)
    end
  }
)
