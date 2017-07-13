local function on_connect(session)
  local from = session:from()
  local to = session:to()

  session:write(
    string.format("Hello session %s:%s from %s:%d to %s:%d\n",
      session.session_id, session.worker_session_id,
      from.ip, from.port, to.ip, to.port
    )
  )
end

local function on_data(session, data)
  -- echo content
  session:write(data)
end

local function on_close(session)
end

local function on_transfer(session)
end

return function(session)
  session.on_connect = on_connect
  session.on_data = on_data
  session.on_close = on_close
  session.on_transfer = on_transfer
end
