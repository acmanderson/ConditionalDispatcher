alias Experimental.GenStage

defmodule ConditionalDispatcher do
  @behaviour GenStage.Dispatcher

  def init(_opts) do
    {:ok, {%{}, %{}, 0, nil}}
  end

  def notify(msg, {demands, _,  _, _} = state) do
    for {name, name_demands} <- demands do
      Enum.each(name_demands, fn {_, pid, ref, _} ->
        Process.send(pid, {:"$gen_consumer", {self(), ref}, {:notification, msg}}, [:noconnect])
      end)
    end
    {:ok, state}
  end

  def subscribe(opts, {pid, ref}, {demands, refs_map, pending, max}) do
    name  = Keyword.get(opts, :name) ||
             raise ArgumentError, "the :name is required when using the conditional dispatcher"
    func  = Keyword.get(opts, :func) ||
             raise ArgumentError, "the :func is required when using the conditional dispatcher"
    {:ok, 0, {Map.put(demands, name, Map.get(demands, name, []) ++ [{0, pid, ref, func}]), Map.put(refs_map, ref, name), pending, max}}
  end

  def cancel({_, ref}, {demands, refs_map, pending, max}) do
    {{current, _, _, _}, name_demands} = pop_demand(ref, demands, refs_map)
    {:ok, 0, {Map.put(demands, refs_map[ref], name_demands), Map.delete(refs_map, ref), current + pending, max}}
  end

  def ask(counter, {pid, ref}, {demands, refs_map, pending, max}) do
    max = max || counter

    if counter > max do
      :error_logger.warning_msg('GenStage producer DemandDispatcher expects a maximum demand of ~p. ' ++
                                'Using different maximum demands will overload greedy consumers. ' ++
                                'Got demand for ~p events from ~p~n', [max, counter, pid])
    end

    # TODO: I'm still a little confused by pending and max and whether they should be split among consumer names,
    # so figure that out sometime.
    {{current, _, _, match_fun}, name_demands} = pop_demand(ref, demands, refs_map)
    demands = Map.put(demands, refs_map[ref], add_demand(current + counter, pid, ref, match_fun, name_demands))

    already_sent = min(pending, counter)
    {:ok, counter - already_sent, {demands, refs_map, pending - already_sent, max}}
  end

  def dispatch(events, {demands, refs_map, pending, max}) do
    {events, demands} = dispatch_demand(events, demands)
    {:ok, events, {demands, refs_map, pending, max}}
  end

  defp dispatch_demand([], demands) do
    {[], demands}
  end

  defp dispatch_demand(events, demands) when is_map(demands) do 
    event_split = for {name, [{counter, pid, ref, match_fun} | name_demands]} <- demands do
      {deliver_now, deliver_later, counter} = 
        split_events(events, counter, [], match_fun)
      {deliver_now, Enum.filter(deliver_later, match_fun), {counter, pid, ref, match_fun}, name, name_demands}
    end

    deliver_later = Enum.map(event_split, fn e -> elem(e, 1) end) |> Enum.reduce([], fn(e, acc) -> acc ++ e end) |> Enum.uniq
    new_demands = for {deliver_now, _, {counter, pid, ref, match_fun}, name, name_demands} <- event_split do
      # TODO: maybe figure out a way for events to be delivered to a consumer if another consumer that wants those events
      # can't receive them, as now it will wait until all consumers can receive the event before sending it.
      new_deliver_now = deliver_now -- deliver_later
      unless Enum.empty?(new_deliver_now), do: Process.send(pid, {:"$gen_consumer", {self(), ref}, new_deliver_now}, [:noconnect])
      {name, add_demand(counter + length(deliver_now) - length(new_deliver_now), pid, ref, match_fun, name_demands)}
    end
    new_demands = Enum.into(new_demands, %{})
    
    case demands == new_demands do
      true -> {events, demands}
      false -> dispatch_demand(deliver_later, new_demands)
    end
  end

  defp split_events(events, 0, acc, match_fun),
    do: {:lists.reverse(acc), events, 0}
  defp split_events([], counter, acc, match_fun),
    do: {:lists.reverse(acc), [], counter}
  defp split_events([event | events], counter, acc, match_fun) do
    case apply(match_fun, [event]) do
      true -> split_events(events, counter - 1, [event | acc], match_fun)
      false -> split_events(events, counter, acc, match_fun)
    end
  end

  defp add_demand(counter, pid, ref, match_fun, [{c, _, _, _} | _] = demands) when counter > c,
    do: [{counter, pid, ref, match_fun} | demands]
  defp add_demand(counter, pid, ref, match_fun, [demand | demands]),
    do: [demand | add_demand(counter, pid, ref, match_fun, demands)]
  defp add_demand(counter, pid, ref, match_fun, []) when is_integer(counter),
    do: [{counter, pid, ref, match_fun}]

  defp pop_demand(ref, demands, refs_map) do
    List.keytake(demands[refs_map[ref]], ref, 2)
  end
end
