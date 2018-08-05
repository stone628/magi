local content_path = ...

package.path = string.format("%s/?.lua;%s", content_path, package.path)

local runner = require("magi.runner")
local DIR_DELIM = string.sub(package.config, 1, 1)

if string.sub(content_path, string.len(content_path)) ~= DIR_DELIM then
  content_path = content_path .. DIR_DELIM
end

return runner(content_path)
