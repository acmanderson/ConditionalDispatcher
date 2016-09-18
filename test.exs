defmodule DispatcherTest do
  def is_odd_or_2(num), do: rem(num, 2) != 0 || num == 4
  def is_even(num), do: rem(num, 2) == 0 || num == 7

  defp split_events(events, 0, acc, func),
    do: {:lists.reverse(acc), events, 0}
  defp split_events([], counter, acc, func),
    do: {:lists.reverse(acc), [], counter}
  defp split_events([event | events], counter, acc, func) do
    case apply(func, [event]) do
      true -> split_events(events, counter - 1, [event | acc], func)
      false -> split_events(events, counter, acc, func)
    end
  end

  def dispatch(events, {demands, pending, max}) do
    {events, demands} = dispatch_demand(events, demands)
    {:ok, events, {demands, pending, max}}
  end

  defp dispatch_demand([], demands) do
    {[], demands}
  end

  # defp dispatch_demand(events, [{0, _, _} | _] = demands) do
  #   {events, demands}
  # end

  defp dispatch_demand(events, demands) when is_map(demands) do
    # events_set = MapSet.new events
    IO.puts inspect demands
    new_events = for {name, [{counter, func, ref} | _]} <- demands do
      {deliver_now, deliver_later, counter} =
        split_events(events, counter, [], func)
      IO.inspect Enum.filter(events, func)
      IO.inspect deliver_now
      IO.inspect deliver_later
      {deliver_now, deliver_later}
    end
    [deliver_now, deliver_later] = List.zip(new_events)
    deliver_now = Tuple.to_list(deliver_now)
    deliver_later = Tuple.to_list(deliver_later) |> Enum.reduce(fn(e, acc) -> acc ++ e end) |> Enum.uniq
    IO.inspect deliver_later
    for event <- deliver_now do
      IO.puts("Preparing #{inspect event}")
      event = event -- deliver_later
      IO.puts("Sending #{inspect event}")
    end
    # IO.puts inspect new_events
    # demands = add_demand(counter, pid, ref, demands)
    # dispatch_demand(deliver_later, demands)
  end

  def demands do
    {ref1, ref2, ref3} = {make_ref(), make_ref(), make_ref()}
    demands = %{
      :foo => [{8, &DispatcherTest.is_even/1, ref1}, {1, &DispatcherTest.is_even/1, ref2}],
      :bar => [{2, &DispatcherTest.is_odd_or_2/1, ref3}]
    }
    refs_map = %{
      ref1 => :foo,
      ref2 => :foo,
      ref3 => :bar,
    }
    {demands, refs_map}
  end
  def events, do: Enum.to_list(1..10)

  def main do
    {demands, refs_map} = demands
    dispatch_demand(events, demands)
  end
end

DispatcherTest.main