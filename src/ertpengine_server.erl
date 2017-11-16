-module(ertpengine_server).

-behaviour(gen_server).

%% API
-export([start_link/2,
         do_command/2
        ]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(cookie_item, {
          id = <<>> :: binary(),
          timestamp = {sec, erlang:monotonic_time(second)}
         }).


-record(state, {
    socket,
    proxy_ip = "127.0.0.1",
    proxy_port = 2223,
    tx_list = gb_trees:empty(),
    cookie_queue = queue:new(),
    cookie_set = #{}
}).

-define(COOKIE_LEN, 16).
-define(COOKIE_EXPIRE, 60).
-define(RETRANSMIT_TIME, 1000).
-define(RETRANSMIT_NUM,  2).

%%%===================================================================
%%% API
%%%===================================================================
start_link(Name, Args) ->
    gen_server:start_link({local, Name}, ?MODULE, Args, []).

do_command(Name, Args) ->
    {{dict, Data}, _} = gen_server:call(Name, {do_command, Args}),
    Data.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init(Args) ->
    {ok, Socket} = gen_udp:open(0, [binary]),
    IP = proplists:get_value(ip, Args, "127.0.0.1"),
    Port = proplists:get_value(port, Args, 2223),
    {ok, #state{socket = Socket, proxy_ip = IP, proxy_port = Port}}.

handle_call({do_command, Args} = Command, From, #state{socket = Socket, proxy_ip = IP, proxy_port = Port} = State) ->
    {Cookie,State1} = cookie(State),
    TimerRef = erlang:start_timer(?RETRANSMIT_TIME, self(), { retransmit, Cookie, Command, From, ?RETRANSMIT_NUM } ),
    Data = bencode(Args),
    gen_udp:send(Socket, IP, Port, <<Cookie/binary, " ", Data/binary>>),
    {noreply, State1#state{tx_list = gb_trees:enter(Cookie, {From, TimerRef}, tx_list(State1))}};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({udp, _, _, _, <<Cookie:?COOKIE_LEN/binary, " ", Data/binary>>}, State) ->
    case gb_trees:lookup(Cookie, tx_list(State)) of
        none ->
            error_logger:error_msg("Not found tx for: ~p ~p", [Cookie, Data]);
        {value, {From, TimerRef}} ->
            erlang:cancel_timer(TimerRef, [ {async, true}, {info, false} ]),
            gen_server:reply(From, bencode:decode(Data))
    end,
    {noreply, delete_from_txlist(Cookie, State)};

handle_info({timeout, TimerRef, {retransmit, Cookie, _Command, From, 0} }, State) ->
    case gb_trees:lookup(Cookie, tx_list(State)) of
        none ->
            {noreply, State};
        {value, {From, TimerRef}} ->
            gen_server:reply(From, { error, timeout }),
            {noreply, delete_from_txlist(Cookie, State)};
        {value, _} ->
            {noreply, State}
    end;
handle_info({timeout, TimerRef, {retransmit, Cookie, {do_command, Args} = Command, From, N} }, State) ->
    case gb_trees:lookup(Cookie, tx_list(State)) of
        none ->
            {noreply, State};
        {value, {From, TimerRef}} ->
            #state{socket = Socket, proxy_ip = IP, proxy_port = Port} = State,
            NewTimerRef = erlang:start_timer(?RETRANSMIT_TIME, self(), {retransmit, Cookie, Command, From, N-1} ),
            Data = bencode(Args),
            gen_udp:send(Socket, IP, Port, <<Cookie/binary, " ", Data/binary>>),
            {noreply, update_tx_list_timer(Cookie, NewTimerRef, State)};
        _ ->
            {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
create_id() ->
    Len = round(?COOKIE_LEN/2),
    << <<Y>> || <<X:4>> <= crypto:strong_rand_bytes(Len),
                Y <- integer_to_list(X, 16) >>.

cookie(State) ->
    Id = create_id(),
    case has_cookie(Id, State) of
        true ->
            cookie(State);
        false ->
            NewState = add_cookie(Id, State),
            {Id, NewState}
    end.

bencode(List) ->
    Dict = lists:foldl(fun({K, V}, D) ->
                           dict:store(K, V, D)
                       end, dict:new(), List),
    Data = {dict, Dict},
    bencode:encode(Data).

has_cookie(Id, #state{ cookie_set = CookieSet }) ->
    maps:get(Id, CookieSet, false).

add_cookie(Id, #state{ cookie_queue = CookieQueue } = State) ->
    case has_cookie(Id, State) of
        true ->
            State;
        false ->
            QueueItem = #cookie_item{id = Id},
            NewQueue  = queue:in(QueueItem, CookieQueue),
            State1 = add_cookie_to_set(Id, State),
            State2 = update_cookie_queue(NewQueue, State1),
            purge_old_cookies(State2)
    end.

purge_old_cookies(State) ->
    Now = erlang:monotonic_time(second),
    purge_old_cookies(Now, State).

purge_old_cookies(Now, #state{ cookie_queue = CookieQueue } = State) ->
    case queue:peek(CookieQueue) of
        empty -> State;
        {value, #cookie_item{id = Id, timestamp = {sec, Time}}} when Now - Time >= ?COOKIE_EXPIRE ->
            State1 = remove_cookie_from_set(Id, State),
            NewQueue = queue:drop(CookieQueue),
            State2 = update_cookie_queue(NewQueue, State1),
            purge_old_cookies(Now, State2);
        _ ->
            State
    end.

tx_list(#state{tx_list = TXList}) ->
    TXList.

remove_cookie_from_set(Id, #state{ cookie_set = CookieSet } = State) ->
    State#state{ cookie_set = maps:remove(Id, CookieSet) }.

add_cookie_to_set(Id, #state{ cookie_set = CookieSet } = State) ->
    State#state{ cookie_set = CookieSet#{ Id => true } }.

update_cookie_queue(CookieQueue, State) ->
    State#state{ cookie_queue = CookieQueue }.

delete_from_txlist(Cookie, State) ->
    State#state{tx_list = gb_trees:delete_any(Cookie, tx_list(State))}.

update_tx_list_timer(Cookie, NewTimerRef, State) ->
    case gb_trees:lookup(Cookie, tx_list(State)) of
        none ->
            State;
        {value,{From, _TimerRef}} ->
            NewTXList = gb_trees:enter(Cookie, {From, NewTimerRef}, tx_list(State)),
            State#state{ tx_list = NewTXList }
    end.
