local cjson = require("cjson")
local zmq = require("lzmq")
local zpoll = require("lzmq.poller")

local log = require "lua/log"

local random = math.random
math.randomseed( os.time() )

local CLIENT = "\x02"
local DEFAULT_TIMEOUT_MS = 5000
local DEFAULT_ATTEMPTS = 2
local DEFAULT_SOCKET_POOL = 100

local SOCKET_POLL_INTERVAL_S = 0.01
local RES_POLL_INTERVAL_S = 0.001


local function now()
   ngx.update_time()
   return ngx.now()
end

local function gen_id()
   local template ='xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'
   return string.gsub(
      template,
      '[xy]',
      function (c)
         local v = (c == 'x') and random(0, 0xf) or random(8, 0xb)
         return string.format('%x', v)
   end)
end

local function do_recv(socket)
   local res, err = socket:recv_all()
   if err then
      return nil, err
   end
   local empty = res[1]
   local res_id = res[2]
   local res = {unpack(res, 3)}
   return res_id, res, err
end


local function single_req(req_id, with_socket, frames, poller, responses, timeout)
   local request = {"", CLIENT, req_id} -- pad bcuz 'DEALER'
   for _, v in pairs(frames) do
      table.insert(request, v)
   end
   local function _send(socket)
      socket:send_all(request)
      local exp_time = now() + (timeout / 1000)
      while responses[req_id] == nil do
         if now() > exp_time then
            responses[req_id] = -1
            log.err("req timeout %s", req_id)
            return nil, "ez timeout"
         end
         poller:poll(1)
         ngx.sleep(RES_POLL_INTERVAL_S)
      end
      if responses[req_id] == -1 then
         log.error("req timed out: %s", req_id)
         return nil, "TIMEOUT"
      end
      local res, err = table.unpack(responses[req_id])
      responses[req_id] = nil
      return res, err
   end
   return with_socket(_send)
end

local function full_request(with_socket, frames, timeout, attempts, poller, responses)
   local req_id = gen_id()
   if not attempts then
      attempts = DEFAULT_ATTEMPTS
   end
   if not timeout then
      timeout = DEFAULT_TIMEOUT_MS
   end
   local attempt = 0
   local res, err
   while attempt < attempts do
      res, err = single_req(
         req_id, with_socket, frames, poller, responses, timeout
      )
      if not err then
         return res, nil
      end
      attempt = attempt + 1
   end
   return res, err
end


local M = {}
function M.new_requester(host, port, socket_pool_count)
   if not socket_pool_count then
      socket_pool_count = DEFAULT_SOCKET_POOL
   end
   local con_s = string.format("tcp://%s:%s", host, port)
   local ctx = zmq:context()
   local poller = zpoll.new(socket_pool_count)
   local socket_pool = {}
   local responses = {}
   for i = 1, socket_pool_count do
      local socket = ctx:socket{
         zmq.DEALER,
         connect = con_s,
      }
      table.insert(socket_pool, socket)
      poller:add(socket, zmq.POLLIN, function(sock)
                    local res_id, res, err = do_recv(sock)
                    if responses[res_id] == -1 then
                       log.err("received late ez response for %s", res_id)
                    else
                       responses[res_id] = {res, err}
                    end

      end)
   end
   log.info("connected %s dealer sockets", socket_pool_count)

   local function with_socket(cb)
      while #socket_pool < 1 do
         ngx.sleep(SOCKET_POLL_INTERVAL_S)
      end
      local socket = table.remove(socket_pool)
      local res, err = cb(socket)
      table.insert(socket_pool, socket)
      return res, err
   end

   local function _req(frames, conf)
      if not conf then
         conf = {}
      end
      local res, err = full_request(
         with_socket, frames, conf.timeout, conf.attempts, poller, responses
      )
      return res, err
   end
   return _req
end
return M
