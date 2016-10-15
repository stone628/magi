local uv = require('luv')
local p = require('utils').prettyPrint

local workers = {}

local function on_worker_close(worker_id, exit_status, term_signal)
  local worker = workers[worker_id]

  uv.close(worker.pipe)

  table.remove(workers, worker_id)

  p("on_worker_close", { worker_id = worker_id, exit_status = exit_status, term_signal = term_signal })

  if #workers == 0 then
    p("all workers closed")
    uv.stop()
  end
end

local worker_impl = string.dump(
  function()
    local uv = require('luv')
    local p = require('utils').prettyPrint
    local worker_id = os.getenv("WORKER_ID")
    local W = string.format("[WORKER%d]", worker_id)
    local conn_count = 0

    p(W, "starting new worker", worker_id)

    local queue = uv.new_pipe(true)

    local function on_client(err, data)
      p(W, "on_client enter", { err = err, data = data })

      if err then
        return
      end

      if uv.pipe_pending_count(queue) > 0 then
        local client = uv.new_tcp()

        if uv.accept(queue, client) then
          local from = uv.tcp_getsockname(client)
          local to = uv.tcp_getpeername(client)

          p(W, "on_client accepted", client, from, to)
          uv.write(client,
            string.format("BYE connection #%d from %s:%d to %s:%d in worker %d!\n",
              conn_count, from.ip, from.port, to.ip, to.port, worker_id
            )
          )
          uv.shutdown(client,
            function()
              p(W, "on_client closing", client, from, to)
              uv.close(client)
            end
          )
          conn_count = conn_count + 1
        else
          p(W, "on_client accept fail", client)
          uv.close(client)
        end
      elseif data then
        p(W, "on_client data", data)
      else
        p(W, "on_client detect end on queue")
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

local function setup_workers()
  local lua_path = uv.exepath()
  local cpu_info = uv.cpu_info()

  p("lua_path", lua_path)

  for i = 1, #cpu_info do
    local pipe = uv.new_pipe(true)
    local input = uv.new_pipe(false)
    local worker_id = i

    local handle, pid = uv.spawn(
      lua_path,
      {
        stdio = { input, 1, 2, pipe },
        env = { string.format("WORKER_ID=%d", i) },
      }, 
      function(exit_status, term_signal)
        on_worker_close(worker_id, exit_status, term_signal)
      end
    )

    workers[i] = {
      id = i,
      pipe = pipe,
      count = 0,
      handle = handle,
    }

    if pid == nil then
      -- spawn fail
      uv.close(pipe)
    else
      -- spawn success
      uv.write(input, worker_impl)
    end

    uv.shutdown(input)
  end
end

setup_workers()
p("setting up workers", workers)

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
    p("failed to send client over pipe")
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

p("install sigint handler")
local sigint = uv.new_signal()

uv.signal_start(sigint, "sigint",
  function(signal)
    p("signal handler with", signal, ", stopping uv")

    for i, worker in ipairs(workers) do
      if worker.handle ~= nil then
        p("sending sigterm to", worker)
        uv.process_kill(worker.handle, "sigterm")
      end
    end

    uv.stop();
  end
)

p("start event loop")
uv.run()
p("done event loop")
uv.loop_close()
