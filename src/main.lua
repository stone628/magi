local uv = require('luv')

local pp = require('utils').prettyPrint
local p = function(...) pp("[MAIN]", ...) end

local shutdown_workers = false
local workers = {}

local worker_impl = string.dump(
  function(...)
    local worker_id = tonumber(arg[1])
    local uv = require('luv')

    local pp = require('utils').prettyPrint
    local W = string.format("[WORKER%02d]", worker_id)
    local p = function(...) pp(W, ...) end

    local conn_count = 0

    p("starting new worker", worker_id, "option", arg)

    local queue = uv.new_pipe(true)

    local function on_client(err, data)
      p("on_client enter", { err = err, data = data })

      if err then
        return
      end

      if uv.pipe_pending_count(queue) > 0 then
        local client = uv.new_tcp()

        if uv.accept(queue, client) then
          local from = uv.tcp_getsockname(client)
          local to = uv.tcp_getpeername(client)

          p("on_client accepted", client, from, to)
          uv.write(client,
            string.format("BYE connection #%d from %s:%d to %s:%d in worker %d!\n",
              conn_count, from.ip, from.port, to.ip, to.port, worker_id
            )
          )
          uv.shutdown(client,
            function()
              p("on_client closing", client, from, to)
              uv.close(client)
            end
          )
          conn_count = conn_count + 1
        else
          p("on_client accept fail", client)
          uv.close(client)
        end
      elseif data then
        p("on_client data", data)
      else
        p("on_client detect end on queue")
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
  end
)

local setup_worker

local function on_worker_close(worker_id, exit_status, term_signal)
  p("on_worker_close", { worker_id = worker_id, exit_status = exit_status, term_signal = term_signal })

  local worker = workers[worker_id]

  if worker.pipe ~= nil then
    uv.close(worker.pipe)
    worker.pipe = nil
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
      p("all workers closed")
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

    p("respawn worker", worker_id, "in 1sec")
  end
end

function setup_worker(lua_path, worker_id)
  local pipe = uv.new_pipe(true)
  local input = uv.new_pipe(false)
  local handle, pid = uv.spawn(
    lua_path,
    {
      args = { "-", worker_id, "option2", },
      stdio = { input, 1, 2, pipe },
      env = { string.format("WORKER_ID=%d", worker_id) },
    }, 
    function(exit_status, term_signal)
      on_worker_close(worker_id, exit_status, term_signal)
    end
  )
  local worker = {
    id = worker_id,
    count = 0,
  }

  workers[worker_id] = worker

  if pid == nil then
    -- spawn fail
    uv.close(pipe)
    p("worker spawn fail", worker)
  else
    -- spawn success
    worker.handle = handle
    worker.pid = pid
    worker.pipe = pipe
    uv.write(input, worker_impl)
    p("worker spawn success", worker)
  end

  uv.shutdown(input)
end

local function setup_workers()
  local lua_path = uv.exepath()
  local cpu_info = uv.cpu_info()

  p("lua_path", lua_path)

  for i = 1, #cpu_info do
    setup_worker(lua_path, i)
  end
end

p("setting up workers")
setup_workers()

local server = uv.new_tcp()
local function on_connect(err)
  assert(not err, err)

  local client = uv.new_tcp()
  local worker_counts = {}

  uv.accept(server, client)

  for i, worker in ipairs(workers) do
    worker_counts[i] = {
      id = i,
      count = worker.count,
    }
  end

  table.sort(worker_counts,
    function(lhs, rhs)
      return lhs.count < rhs.count
    end
  )

  local worker = workers[worker_counts[1].id]

  p("worker counts", worker_counts, "selected", worker)

  if uv.write2(worker.pipe, "123", client) then
    worker.count = worker.count + 1
  else
    p(W, "failed to send client over pipe")
    uv.shutdown(client)
    uv.close(client)
  end 
end

p("setting up server socket", server)
if uv.tcp_bind(server, "0.0.0.0", 0) then
  p("server socket bound, listening...", server, uv.tcp_getsockname(server))
  uv.listen(server, 128, on_connect)
else
  uv.stop()
end

p("installing sigint handler")
local sigint = uv.new_signal()

uv.signal_start(sigint, "sigint",
  function(signal)
    p("signal handler with", signal, ", shutdown workers...")

    shutdown_workers = true

    for i, worker in ipairs(workers) do
      if worker.handle ~= nil then
        p("sending sigterm to", worker)
        uv.process_kill(worker.handle, "sigterm")
      end
    end
  end
)

p("start event loop")
uv.run()
p("done event loop")
uv.loop_close()
