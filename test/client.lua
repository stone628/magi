package.path = '../src/?.lua;' .. package.path

local uv = require('luv')
local uuid = require('uuid')

local logger = require('logger')
local config = require('config')

logger.level = "debug"

local args = { ... }
local stdin = uv.new_tty(0, true)
local sessions = {}

local function on_stdin_read(err, data)
  if err then
    logger.error("on_stdin_read error", err)
    return
  end

  if data then
    local i = string.find(data, ' ')

    if i then
      local session_id = string.sub(data, 1, i - 1)
      local send_data = string.sub(data, i + 1)

      logger.debug("on_stdin_read parsed", { session_id = session_id, data = send_data})
  
      if session_id == "*" then
        for sid, session in pairs(sessions) do
          if session then
            logger.debug("on_stdin_read sending data", { session_id = sid, data = send_data })
            session.send(send_data)
          end
        end
      else
        local session = sessions[session_id]
  
        if session then
          logger.debug("on_stdin_read sending data", { session_id = session_id, data = send_data })
          session.send(send_data)
        end
      end
    end
    return
  end

  logger.info("on_stdin_read disconnected")
end

local function create_session(client)
  local new_session = {
    session_id = uuid(),
    connection = client,
    data = {},
    from = uv.tcp_getsockname(client),
    to = uv.tcp_getpeername(client),
  }

  new_session.send = function(data)
    uv.write(new_session.connection, data)
  end

  sessions[new_session.session_id] = new_session
  return new_session
end

local function on_client_read(session, err, data)
  if err then
    logger.error("on_client_read error", err, { session_id = session.session_id, from = session.from, to = session.to })
    return
  end

  if data then
    logger.debug("on_client_read received", { session_id = session.session_id, data = data })
    return
  end

  logger.info("on_client_read disconnected", { session_id = session.session_id, from = session.from, to = session.to })
  sessions[session.session_id] = nil
end

local function spawn_client()
  local client = uv.new_tcp()
  local result = uv.tcp_connect(client, "127.0.0.1", config.SERVER_PORT,
    function(err)
      if not err then
        local session = create_session(client)

        uv.read_start(client,
          function(err, data) on_client_read(session, err, data) end
        )

        logger.info("client spawn", { session_id = session.session_id, from = session.from, to = session.to })
        return
      end

      logger.error("failed to connect to server", err)
      uv.close(client)
    end
  )

  if not result then
    logger.error("failed to connect to server", config.SERVER_PORT)
    uv.stop()
  end
end

--------------------------------------------------------------------------------
-- client main logic
--------------------------------------------------------------------------------
logger.info("starting magi client, version", 0.1)

uuid.seed()

uv.signal_start(uv.new_signal(), "sigint",
  function(signal)
    logger.info("received SIGINT")
    uv.stop()
  end
)

local client_size = tonumber(args[1] or 1)
local spawn_count = 0
local spawn_timer = uv.new_timer()

uv.timer_start(spawn_timer, 100, 100,
  function()
    spawn_count = spawn_count + 1
    logger.info("spawning client", spawn_count)
    spawn_client()

    if spawn_count >= client_size then
      uv.timer_stop(spawn_timer)
      uv.close(spawn_timer)
    end
  end
)

uv.read_start(stdin, on_stdin_read)

logger.info("start event loop")
uv.run()
logger.info("done event loop")
logger.flush()
uv.loop_close()
