%%%-------------------------------------------------------------------
%%% @author kishorebaktha
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 06. Oct 2018 12:53 PM
%%%-------------------------------------------------------------------
-module(node3).
-export([start/1, start/2]).

-define(Stabilize, 1000).
-define(Timeout, 10000).

start(Id) ->
  start(Id, nil).

start(Id, Peer) ->
  timer:start(),
  spawn(fun() -> init(Id, Peer) end).

init(Id, Peer) ->
  Predecessor = nil,
  Next=nil,
  {ok, Successor} = connect(Id, Peer),
  schedule_stabilize(),
  io:format("reached here~n"),
  Store = storage:create(),
  node(Id, Predecessor, Successor,Store,Next).

connect(Id, nil) ->
  Ref=monitor(self()),
  {ok, {Id, Ref,self()}};


connect(Id, Peer) ->
  io:format("received  ~w~n",[Id]),
  Qref = make_ref(),
  Peer ! {key, Qref, self()},
  receive
    {Qref, Skey} ->
      io:format("received~w~n",[Skey]),
      Ref=monitor(Peer),
      {ok, {Skey,Ref, Peer}}
  after ?Timeout ->
    io:format("Time out: no response~n", [])
  end.


node(Id, Predecessor, Successor,Store,Next) ->
  receive
  % A peer needs to know our key Id
    {key, Qref, Peer} ->
      Peer ! {Qref, Id},
      node(Id, Predecessor, Successor,Store,Next);
  % New node
    {notify, New} ->
      {Pred,Store2} = notify(New, Id, Predecessor,Store),
      node(Id, Pred, Successor,Store2,Next);
  % Message coming from the predecessor who wants to know our predecessor
    {request, Peer} ->
      request(Peer, Predecessor,Successor),
      node(Id, Predecessor, Successor,Store,Next);
  % What is the predecessor of the next node (successor)
    {status, Pred, Nx} ->
      {Succ, Next2} = stabilize(Pred, Nx, Id, Successor),
      node(Id, Predecessor, Succ, Store,Next2);
    stabilize ->
      % io:format("ID stab ~w",[Id]),
      stabilize(Successor),
      node(Id, Predecessor, Successor,Store,Next);
    probe ->
      io:format("received~n"),
      create_probe(Id, Successor),
      node(Id, Predecessor, Successor,Store,Next);
    {probe, Id, Nodes, T} ->
      remove_probe(T, Nodes),
      node(Id, Predecessor, Successor,Store,Next);
    {probe, Ref, Nodes, T} ->
      forward_probe(Ref, T, Nodes, Id, Successor),
      node(Id, Predecessor, Successor,Store,Next);

    state ->
      io:format(' Id : ~w~n Predecessor : ~w~n Successor : ~w~n Store : ~w~n Next : ~w~n ', [Id, Predecessor, Successor,Store,Next]),
      node(Id, Predecessor, Successor,Store,Next);

    stop -> io:format("received stop"),
      ok;

    {add, Key, Value, Qref, Client} ->
      Added = add(Key, Value, Qref, Client,
        Id, Predecessor, Successor, Store),
      node(Id, Predecessor, Successor, Added,Next);

    {lookup, Key, Qref, Client} ->
     % io:format("Store-~w~n",[Store]),
      lookup(Key, Qref, Client, Id, Predecessor, Successor, Store),
      node(Id, Predecessor, Successor, Store,Next);


    {handover, Elements} ->
      Merged = storage:merge(Store, Elements),
      node(Id, Predecessor, Successor, Merged,Next);

    {'DOWN', Ref, process, _, _} ->
{Pred, Succ, Nxt}  = down(Ref, Predecessor, Successor, Next),
node(Id, Pred, Succ, Store,Nxt);

    _ ->
      io:format('Strange message received'),
      node(Id, Predecessor, Successor,Store,Next)
  end.

stabilize({_,_, Spid}) ->
  Spid ! {request, self()}.

add(Key, Value, Qref, Client, Id, {Pkey, _,_}, {_,_, Spid}, Store) ->
  case key:between(Key,Pkey,Id)  of
true ->
  Added=storage:add(Key,Value,Store),
  Client ! {Qref, ok},
  Added;
 false ->
   Spid ! {add, Key, Value, Qref, Client},
   Store
 end.

lookup(Key, Qref, Client, Id, {Pkey, _,_}, Successor, Store) ->
  case key:between(Key,Pkey,Id) of
true ->
  io:format("Store2-~w~n",[Store]),
Result = storage:lookup(Key, Store),
Client ! {Qref, Result};
false ->
{_,_, Spid} = Successor,
  Spid ! {lookup, Key, Qref, Client}
  end.

stabilize(Pred,Next, Id, Successor) ->
  {Skey,Sref, Spid} = Successor,
  case Pred of
    nil ->
      % io:format("request1~n"),
      Spid ! {notify, {Id, self()}},
      {Successor,Next};
    {Id,_} ->
      {Successor,Next};
    {Skey,_} ->
      %  io:format("request2~n"),
      Spid ! {notify, {Id, self()}},
      {Successor,Next};
    {Xkey, Xpid} ->
      case key:between(Xkey, Id, Skey) of
        true ->
          %  io:format("request3~n"),
          Xpid ! {request, self()},
          drop(Sref),
          Ref=monitor(Xpid),
          {{Xkey,Ref,Xpid},Successor};
        false ->
          Spid ! {notify, {Id, self()}},
          {Successor,Next}
      end
  end.

schedule_stabilize() ->
  timer:send_interval(?Stabilize, self(), stabilize).

request(Peer, Predecessor,{Skey,_,Spid}) ->
  case Predecessor of
    nil ->
      Peer ! {status, nil,{Skey,Spid}};
    {Pkey,_, Ppid} ->
      Peer ! {status, {Pkey, Ppid},{Skey,Spid}}
  end.

notify({Nkey, Npid}, Id, Predecessor, Store) ->
  case Predecessor of
    nil ->
      Keep = handover(Id, Store, Nkey, Npid),
      Ref=monitor(Npid),
      {{Nkey,Ref, Npid},Keep};
       {Pkey, Pref,_} ->
case key:between(Nkey, Pkey, Id) of
true ->
  Keep = handover(Id, Store, Nkey, Npid),
  drop(Pref),
  Ref=monitor(Npid),
  {{Nkey,Ref, Npid},Keep};
  false ->{Predecessor,Store}
end
 end.

handover(Id, Store, Nkey, Npid) ->
  {Keep,Rest} = storage:split(Id, Nkey, Store),
Npid ! {handover, Rest},
Keep.

down(Ref, {_, Ref, _}, Successor, Next) ->
  {nil,Successor,Next};
down(Ref, Predecessor, {_, Ref, _}, {Nkey,Npid}) ->
  Nref=monitor(Npid),
  {Predecessor, {Nkey, Nref, Npid}, nil}.


create_probe(Id,{_,_,Spid}) ->
  Spid ! {probe,Id,[Id],erlang:now()}.

remove_probe(T, Nodes) ->
  Time = timer:now_diff(erlang:now(),T),
  NodeList = fun(E) -> io:format("~p ",[E]) end,
  lists:foreach(NodeList,Nodes),
  io:format("~n Time = ~p",[Time]).

forward_probe(Ref, T, Nodes, Id, {_,_,Spid}) ->
  Spid ! {probe,Ref,Nodes ++ [Id],T}.

monitor(Pid) ->
  erlang:monitor(process, Pid).
drop(nil) ->
  ok;
drop(Pid) ->
  erlang:demonitor(Pid, [flush]).