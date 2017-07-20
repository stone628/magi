package.path = '../src/?.lua;' .. package.path

local uv = require('luv')
local uuid = require('uuid')

local logger = require('logger')
local config = require('config')

logger.level = "debug"

local stdin = uv.new_tty(0, true)
local clients = {}

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
      local session = clients[session_id]

      logger.debug("on_stdin_read parsed", { session_id = session_id, data = send_data})

      if session then
        logger.debug("on_stdin_read sending data", { session_id = session_id, data = send_data })
        session.send(send_data)
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

  clients[new_session.session_id] = new_session
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
  clients[session.session_id] = nil
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

uv.timer_start(uv.new_timer(), 0, 0,
  function()
    spawn_client()
  end
)

uv.read_start(stdin, on_stdin_read)

logger.info("start event loop")
uv.run()
logger.info("done event loop")
logger.flush()
uv.loop_close()
