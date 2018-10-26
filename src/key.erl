%%%-------------------------------------------------------------------
%%% @author kishorebaktha
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 05. Oct 2018 12:05 PM
%%%-------------------------------------------------------------------
-module(key).
-author("kishorebaktha").
%% API
-compile(export_all).
generate()->
  random:uniform(1000000000)-1.
between(Key,From,To)->
  if From<To->
    if Key>From,Key=<To->
      true;
      true->false
      end;
     From > To->
     if Key>From->
      true;
      true->
        if Key =<To->
          true;
          true->false
         end
        end;
        true->true
    end.




