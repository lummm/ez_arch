local zmq = require "lzmq"


local CLIENT = "\x02"
local DEFAULT_TIMEOUT_MS = 3000
local DEFAULT_ATTEMPTS = 3


local function single_req(req_id, socket, frames, timeout)
   if not timeout then
      timeout = DEFAULT_TIMEOUT_MS
   end
   local request = {b"", CLIENT, req_id} -- pad bcuz 'DEALER'
   for _, v in pairs(frames) do
      table.insert(request, v)
   end
   socket:send_all(request)
   local res, err = socket:recv_all()
   socket:setsockopt()
   socket:close()
   return res, err
end

local function full_request(socket, frames, timeout, attempts)
   local req_id = ngx.now()
   if not attempts then
      attempts = DEFAULT_ATTEMPTS
   end
   local attempt = 0
   local res, err
   while attempt < attempts do
      res, err = single_req(req_id, socket, frames, timeout)
      if not err then
         break
      end
      attempt = attempt + 1
   end
   return res, err
end

local function new_requester(host, port)
   local ctx = zmq:context()
   local con_s = string.format("tcp://%s:%s", host, port)
   local socket = ctx:socket{
      zmq.DEALER,
      connect = con_s
   }
   local function _req(frames, conf)
      if not conf then
         conf = {}
      end
      return full_request(
         socket, frames, conf.timeout, conf.attempts
      )
   end
   return _req
end

local M = {}
M.new_requester = new_requester
return M
