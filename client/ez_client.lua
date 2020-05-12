local zmq = require "lzmq"


DEFAULT_TIMEOUT_MS = 3000
DEFAULT_ATTEMPTS = 3

local function single_req(ctx, host, port, frames, timeout)
   if not timeout then
      timeout = DEFAULT_TIMEOUT_MS
   end
   local client = ctx:socket{
      zmq.REQ,
      connect = string.format("tcp://%s:%s", host, port),
      rcvtimeo = timeout
   }
   client:send_all(frames)
   return client:recv_all()
end

local function full_request(ctx, host, port, frames, timeout, attempts)
   if not attempts then
      attempts = DEFAULT_ATTEMPTS
   end
   local attempt = 0
   local res, err
   while attempt < attempts do
      res, err = single_req(ctx, host, port, frames, timeout)
      if not err then
         break
      end
      attempt = attempt +1
   end
   return res, err
end

local function new_requester(host, port)
   local ctx = zmq:context()
   local function _req(frames, conf)
      return full_request(
         ctx, host, port, frames, arg.timeout, arg.attempts
      )
   end
   return _req
end

local M = {}
M.new_requester = new_requester
return M
