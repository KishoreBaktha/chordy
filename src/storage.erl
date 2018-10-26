%%%-------------------------------------------------------------------
%%% @author kishorebaktha
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 06. Oct 2018 12:30 PM
%%%-------------------------------------------------------------------
-module(storage).
-author("kishorebaktha").

%% API
-compile(export_all).
create()->
  [].
add(Key,Value,Store)->
  L=[{Key,Value}],
  append(Store,L).

append([H|T], Tail) ->
  [H|append(T, Tail)];
append([], Tail) ->
  Tail.

lookup(Key, Store)->
  case lists:keyfind(Key, 1, Store) of
    {Key,Value} ->{Key,Value};
    false -> false
  end.

split(From, To, Store)->
  Listnew=proplists:get_keys(Store),
  {Listkeysout,Listkeysin}=checkbetween(Listnew,From,To,[],[]),
  Updated=get_values(Listkeysin,Store,[]),
  Rest=get_values(Listkeysout,Store,[]),
  {Updated,Rest}.


checkbetween(List,From,To,List1,List2)->
 case List of
[H|T]->
  M= key:between(H,From,To),
  if M==true->
    Listnew=append([H],List1),
    checkbetween(T,From,To,Listnew,List2);
    true->
      Listnew=append([H],List2),
      checkbetween(T,From,To,List1,Listnew)
  end;
   []->{List1,List2}
 end.

get_values(Listkeys,Store,List)->
  case Listkeys of
    [H|T]->
      case lists:keyfind(H, 1, Store) of
        {Key,Value} ->L=[{Key,Value}],
          Listnew=append(L,List),
          get_values(T,Store,Listnew);
        false -> false
      end;
    []->List
  end.

merge(Entries, Store)->
  case Entries of
    [H|T]-> {Key,Value}=H,
            Storenew=append([{Key,Value}],Store),
            merge(T,Storenew);
    []->Store
  end.






