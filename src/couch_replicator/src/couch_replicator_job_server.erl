% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_replicator_job_server).


-behaviour(gen_server).


-export([
    start_link/1
]).

-export([
    accepted/2
]).

-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3
]).

-define(MAX_ACCEPTORS, 2).
-define(MAX_JOBS, 500).
-define(MAX_CHURN, 100).
-define(INTERVAL_SEC, 15).
-define(MIN_RUN_TIME_SEC, 60).


start_link(Timeout) when is_integer(Timeout) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Timeout, []).


% Callback used by acceptors after they accepted a job and become a worker
%
accepted(Worker, Normal) when is_pid(Worker), is_boolean(Normal) ->
    gen_server:call(?MODULE, {accepted, Worker, Normal}, infinity).


init(Timeout) when is_integer(Timeout) ->
    process_flag(trap_exit, true),
    couch_replicator_jobs:set_timeout(),
    St = #{
        acceptors => #{},
        workers => #{},
        churn => 0,
        config => get_config(),
        timer => undefined,
        timeout => Timeout
    },
    St1 = spawn_acceptors(St),
    St2 = do_send_after(St1),
    {ok, St2}.


terminate(_, #{} = St) ->
    #{
        workers := Workers,
        timeout := Timeout
    } = St,
    [stop_job(Pid) || Pid <- maps:keys(Workers)],
    % Give jobs a chance to checkpoint and release their locks
    wait_jobs_exit(Workers, Timeout),
    ok.


handle_call({accepted, Pid, Normal}, _From, #{} = St) ->
    #{
        acceptors := Acceptors,
        workers := Workers,
        churn := Churn
    } = St,
    case maps:is_key(Pid, Acceptors) of
        true ->
            Val = {Normal, erlang:system_time(second)},
            St1 = St#{
                acceptors := maps:remove(Pid, Acceptors),
                workers := Workers#{Pid => Val},
                churn := Churn + 1
            },
            {reply, ok, spawn_acceptors(St1)};
        false ->
            LogMsg = "~p : unknown acceptor processs ~p",
            couch_log:error(LogMsg, [?MODULE, Pid]),
            {stop, {unknown_acceptor_pid, Pid}, St}
    end;

handle_call(Msg, _From, St) ->
    {stop, {bad_call, Msg}, {bad_call, Msg}, St}.


handle_cast(Msg, St) ->
    {stop, {bad_cast, Msg}, St}.


handle_info(reschedule, #{} = St) ->
    St1 = cancel_timer(St),
    St2 = St1#{config := get_config()},
    St3 = trim_jobs(St2),
    St4 = reschedule(St3),
    St5 = update_stats(St4),
    St6 = do_send_after(St5),
    St7 = St6#{churn := 0},
    {noreply, St7};

handle_info({'EXIT', Pid, Reason}, #{} = St) ->
    #{
        acceptors := Acceptors,
        workers := Workers
    } = St,
    case {maps:is_key(Pid, Acceptors), maps:is_key(Pid, Workers)} of
        {true, false} -> handle_acceptor_exit(St, Pid, Reason);
        {false, true} -> handle_worker_exit(St, Pid, Reason);
        {false, false} -> handle_unknown_exit(St, Pid, Reason)
    end;

handle_info(Msg, St) ->
    {stop, {bad_info, Msg}, St}.


code_change(_OldVsn, St, _Extra) ->
    {ok, St}.


% Scheduling logic

do_send_after(#{} = St) ->
    #{config := #{interval_sec := IntervalSec}} = St,
    IntervalMSec = IntervalSec * 1000,
    Jitter = IntervalMSec div 3,
    WaitMSec = IntervalMSec + rand:uniform(max(1, Jitter)),
    TRef = erlang:send_after(WaitMSec, self(), reschedule),
    St#{timer := TRef}.


cancel_timer(#{timer := undefined} = St) ->
    St;

cancel_timer(#{timer := TRef} = St) when is_reference(TRef) ->
    erlang:cancel_timer(TRef),
    St#{timer := undefined}.


reschedule(#{} = St) ->
    #{
        churn := Churn,
        acceptors := Acceptors,
        workers := Workers,
        config := #{max_jobs := MaxJobs, max_churn := MaxChurn}
    } = St,

    ACnt = maps:size(Acceptors),
    WCnt = maps:size(Workers),

    ChurnLeft = MaxChurn - Churn,
    Slots = (MaxJobs + MaxChurn) - (ACnt + WCnt),
    MinSlotsChurn = min(Slots, ChurnLeft),

    Pending = if MinSlotsChurn =< 0 -> 0; true ->
        % Don't fetch pending if we don't have enough slots of churn budget
        couch_replicator_jobs:pending_count(undefined)
    end,

    % Start new acceptors only if we have churn budget, there are pending jobs
    % and we won't start more than max jobs + churn total acceptors
    ToStart = max(0, lists:min([ChurnLeft, Pending, Slots])),

    lists:foldl(fun(_, StAcc) ->
        {ok, Pid} = couch_replicator_job:start_link(),
        StAcc#{acceptors := Acceptors#{Pid => true}}
    end, St, lists:seq(1, ToStart)).


update_stats(#{} = St) ->
    ACnt = maps:size(maps:get(acceptors, St)),
    WCnt = maps:size(maps:get(workers, St)),
    couch_stats:update_gauge([couch_replicator, jobs, accepting], ACnt),
    couch_stats:update_gauge([couch_replicator, jobs, running], WCnt),
    couch_stats:increment_counter([couch_replicator, jobs, reschedules]),
    St.


trim_jobs(#{} = St) ->
    #{
        workers := Workers,
        churn := Churn,
        config := #{max_jobs := MaxJobs}
    } = St,
    Excess = max(0, maps:size(Workers) - MaxJobs),
    case Excess > 0 of
        true -> couch_log:notice("~p : Stopping excess jobs ~p", [?MODULE, Excess]);
        false -> ok
    end,
    lists:foreach(fun stop_job/1, stop_candidates(St, Excess)),
    St#{churn := Churn + Excess}.


stop_candidates(#{}, Top) when is_integer(Top), Top =< 0 ->
    [];

stop_candidates(#{} = St, Top) when is_integer(Top), Top > 0 ->
    #{
        workers := Workers,
        config := #{min_run_time_sec := MinRunTime}
    } = St,

    WList1 = maps:to_list(Workers), % [{Pid, {Normal, StartTime}},...]

    % Filter out normal jobs and those which have just started running
    MaxT = erlang:system_time(second) - MinRunTime,
    WList2 = lists:filter(fun({_Pid, {Normal, T}}) ->
        not Normal andalso T < MaxT
    end, WList1),

    Sorted = lists:keysort(2, WList2),
    lists:map(fun({Pid, _}) -> Pid end, Sorted).


stop_job(Pid) when is_pid(Pid) ->
    % Replication jobs handle the shutdown signal and then checkpoint in
    % terminate handler
    exit(Pid, shutdown).


wait_jobs_exit(#{} = Jobs, _) when map_size(Jobs) =:= 0 ->
    ok;

wait_jobs_exit(#{} = Jobs, Timeout) ->
    receive
        {'EXIT', Pid, _} ->
            wait_jobs_exit(maps:remove(Pid, Jobs), Timeout)
    after
        Timeout ->
            LogMsg = "~p : ~p jobs didn't terminate cleanly",
            couch_log:error(LogMsg, [?MODULE, map_size(Jobs)]),
            ok
    end.


spawn_acceptors(St) ->
    #{
        workers := Workers,
        acceptors := Acceptors,
        config := #{max_jobs := MaxJobs, max_acceptors := MaxAcceptors}
    } = St,
    ACnt = maps:size(Acceptors),
    WCnt = maps:size(Workers),
    case ACnt < MaxAcceptors andalso (ACnt + WCnt) < MaxJobs of
        true ->
            {ok, Pid} = couch_replicator_job:start_link(),
            NewSt = St#{acceptors := Acceptors#{Pid => true}},
            spawn_acceptors(NewSt);
        false ->
            St
    end.


% Worker process exit handlers

handle_acceptor_exit(#{acceptors := Acceptors} = St, Pid, Reason) ->
    St1 = St#{acceptors := maps:remove(Pid, Acceptors)},
    LogMsg = "~p : acceptor process ~p exited with ~p",
    couch_log:error(LogMsg, [?MODULE, Pid, Reason]),
    {noreply, spawn_acceptors(St1)}.


handle_worker_exit(#{workers := Workers} = St, Pid, Reason) ->
    St1 = St#{workers := maps:remove(Pid, Workers)},
    case Reason of
        normal ->
            ok;
        shutdown ->
            ok;
        {shutdown, _} ->
            ok;
        _ ->
            LogMsg = "~p : replicator job process ~p exited with ~p",
            couch_log:error(LogMsg, [?MODULE, Pid, Reason])
    end,
    {noreply, spawn_acceptors(St1)}.


handle_unknown_exit(St, Pid, Reason) ->
    LogMsg = "~p : unknown process ~p exited with ~p",
    couch_log:error(LogMsg, [?MODULE, Pid, Reason]),
    {stop, {unknown_pid_exit, Pid}, St}.


get_config() ->
    Defaults = #{
        max_acceptors => ?MAX_ACCEPTORS,
        interval_sec => ?INTERVAL_SEC,
        max_jobs => ?MAX_JOBS,
        max_churn => ?MAX_CHURN,
        min_run_time_sec => ?MIN_RUN_TIME_SEC
    },
    maps:map(fun(K, Default) ->
        config:get_integer("replicator", atom_to_list(K), Default)
    end, Defaults).
