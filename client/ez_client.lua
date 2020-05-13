local zmq = require "lzmq"
local CLIENT = "\x02"
local DEFAULT_TIMEOUT_MS = 3000
local DEFAULT_ATTEMPTS = 3

local function single_req(ctx, host, port, frames, timeout)
   if not timeout then
      timeout = DEFAULT_TIMEOUT_MS
   end
   local con_s = string.format("tcp://%s:%s", host, port)
   log.info("connecting to %s", con_s)
   local client, err = ctx:socket{
      zmq.REQ,
      connect = con_s,
      rcvtimeo = timeout
   }
   if err then
      log.err("BAD %s", err)
   end
   local request = {CLIENT}
   for _, v in pairs(frames) do
      table.insert(request, v)
   end
   client:send_all(request)
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
      if not conf then
         conf = {}
      end
      return full_request(
         ctx, host, port, frames, conf.timeout, conf.attempts
      )
   end
   return _req
end

local M = {}
M.new_requester = new_requester
return M
