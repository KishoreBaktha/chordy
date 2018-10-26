%%%-------------------------------------------------------------------
%%% @author kishorebaktha
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 06. Oct 2018 9:29 AM
%%%-------------------------------------------------------------------
-module(node2).
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
  {ok, Successor} = connect(Id, Peer),
  schedule_stabilize(),
  %io:format("reached here~n"),
  Store = storage:create(),
  node(Id, Predecessor, Successor,Store).

connect(Id, nil) ->
  {ok, {Id, self()}};

connect(Id, Peer) ->
  io:format("received  ~w~n",[Id]),
  Qref = make_ref(),
  Peer ! {key, Qref, self()},
  receive
    {Qref, Skey} ->
     % io:format("received~w~n",[Skey]),
      {ok, {Skey, Peer}}
  after ?Timeout ->
    io:format("Time out: no response~n", [])
  end.


node(Id, Predecessor, Successor,Store) ->
  receive
  % A peer needs to know our key Id
    {key, Qref, Peer} ->
      Peer ! {Qref, Id},
      node(Id, Predecessor, Successor,Store);
  % New node
    {notify, New} ->
      {Pred,Store2} = notify(New, Id, Predecessor,Store),
      node(Id, Pred, Successor,Store2);
  % Message coming from the predecessor who wants to know our predecessor
    {request, Peer} ->
      request(Peer, Predecessor),
      node(Id, Predecessor, Successor,Store);
  % What is the predecessor of the next node (successor)
    {status, Pred} ->
      %  io:format("status ~w~n",[Id]),
      Succ = stabilize(Pred, Id, Successor),
      node(Id, Predecessor, Succ,Store);
    stabilize ->
      % io:format("ID stab ~w",[Id]),
      stabilize(Successor),
      node(Id, Predecessor, Successor,Store);
    probe ->
      io:format("received~n"),
      create_probe(Id, Successor),
      node(Id, Predecessor, Successor,Store);
    {probe, Id, Nodes, T} ->
      remove_probe(T, Nodes),
      node(Id, Predecessor, Successor,Store);
    {probe, Ref, Nodes, T} ->
      forward_probe(Ref, T, Nodes, Id, Successor),
      node(Id, Predecessor, Successor,Store);

    state ->
      io:format(' Id : ~w~n Predecessor : ~w~n Successor : ~w~n Store : ~w~n ', [Id, Predecessor, Successor,Store]),
      node(Id, Predecessor, Successor,Store);

    stop -> io:format("received stop"),
      ok;

    {add, Key, Value, Qref, Client} ->
      Added = add(Key, Value, Qref, Client,
        Id, Predecessor, Successor, Store),
      node(Id, Predecessor, Successor, Added);

    {lookup, Key, Qref, Client} ->
      %io:format("Store-~w~n",[Store]),
      lookup(Key, Qref, Client, Id, Predecessor, Successor, Store),
      node(Id, Predecessor, Successor, Store);


    {handover, Elements} ->
      Merged = storage:merge(Store, Elements),
      node(Id, Predecessor, Successor, Merged);

    _ ->
      io:format('Strange message received'),
      node(Id, Predecessor, Successor,Store)
  end.

stabilize({_, Spid}) ->
  Spid ! {request, self()}.

add(Key, Value, Qref, Client, Id, {Pkey, _}, {_, Spid}, Store) ->
  case key:between(Key,Pkey,Id)  of
    true ->
      Added=storage:add(Key,Value,Store),
      Client ! {Qref, ok},
      Added;
    false ->
      Spid ! {add, Key, Value, Qref, Client},
      Store
  end.

lookup(Key, Qref, Client, Id, {Pkey, _}, Successor, Store) ->
  case key:between(Key,Pkey,Id) of
    true ->
      io:format("Store2-~w~n",[Store]),
      Result = storage:lookup(Key, Store),
      Client ! {Qref, Result};
    false ->
      {_, Spid} = Successor,
      Spid ! {lookup, Key, Qref, Client}
  end.

stabilize(Pred, Id, Successor) ->
  {Skey, Spid} = Successor,
  case Pred of
    nil ->
      % io:format("request1~n"),
      Spid ! {notify, {Id, self()}},
      Successor;
    {Id, _} ->
      Successor;
    {Skey, _} ->
      %  io:format("request2~n"),
      Spid ! {notify, {Id, self()}},
      Successor;
    {Xkey, Xpid} ->
      case key:between(Xkey, Id, Skey) of
        true ->
          %  io:format("request3~n"),
          Xpid ! {request, self()},
          Pred;
        false ->
          Spid ! {notify, {Id, self()}},
          Successor
      end
  end.

schedule_stabilize() ->
  timer:send_interval(?Stabilize, self(), stabilize).

request(Peer, Predecessor) ->
  case Predecessor of
    nil ->
      Peer ! {status, nil};
    {Pkey, Ppid} ->
      Peer ! {status, {Pkey, Ppid}}
  end.

notify({Nkey, Npid}, Id, Predecessor, Store) ->
  case Predecessor of
    nil ->
      Keep = handover(Id, Store, Nkey, Npid),
      {{Nkey, Npid},Keep};
    {Pkey, _} ->
      case key:between(Nkey, Pkey, Id) of
        true ->
          Keep = handover(Id, Store, Nkey, Npid),
          {{Nkey, Npid},Keep};
        false ->{Predecessor,Store}
      end
  end.

handover(Id, Store, Nkey, Npid) ->
  {Keep,Rest} = storage:split(Id, Nkey, Store),
  Npid ! {handover, Rest},
  Keep.


create_probe(Id,{_,Spid}) ->
  Spid ! {probe,Id,[Id],erlang:now()}.

remove_probe(T, Nodes) ->
  Time = timer:now_diff(erlang:system_time(micro_seconds),T),
  NodeList = fun(E) -> io:format("~p ",[E]) end,
  lists:foreach(NodeList,Nodes),
  io:format("~n Time = ~p",[Time]).

forward_probe(Ref, T, Nodes, Id, {_,Spid}) ->
  Spid ! {probe,Ref,Nodes ++ [Id],T}.

