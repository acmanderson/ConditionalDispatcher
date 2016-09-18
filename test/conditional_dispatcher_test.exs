defmodule ConditionalDispatcherTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog
  alias ConditionalDispatcher, as: D

  defp dispatcher(opts) do
    {:ok, {%{}, %{}, 0, nil} = state} = D.init(opts)
    state
  end

  defp true_fun(_event), do: true
  defp false_fun(_event), do: false

  test "subscribes and cancels" do
    pid  = self()
    ref  = make_ref()
    disp = dispatcher([])
    match_fun = &true_fun/1

    {:ok, 0, disp} = D.subscribe([name: :test, func: match_fun], {pid, ref}, disp)
    assert disp == {%{:test => [{0, pid, ref, match_fun}]}, %{ref => :test}, 0, nil}

    {:ok, 0, disp} = D.cancel({pid, ref}, disp)
    assert disp == {%{:test => []}, %{}, 0, nil}
  end

  test "subscribes, asks and cancels" do
    pid  = self()
    ref  = make_ref()
    disp = dispatcher([])
    match_fun = &true_fun/1

    # Subscribe, ask and cancel and leave some demand
    {:ok, 0, disp} = D.subscribe([name: :hello, func: match_fun], {pid, ref}, disp)
    assert disp == {%{:hello => [{0, pid, ref, match_fun}]}, %{ref => :hello}, 0, nil}

    {:ok, 10, disp} = D.ask(10, {pid, ref}, disp)
    assert disp == {%{:hello => [{10, pid, ref, match_fun}]}, %{ref => :hello}, 0, 10}

    {:ok, 0, disp} = D.cancel({pid, ref}, disp)
    assert disp == {%{:hello => []}, %{}, 10, 10}

    match_fun = &true_fun/1

    # Subscribe, ask and cancel and leave the same demand
    {:ok, 0, disp} = D.subscribe([name: :world, func: match_fun], {pid, ref}, disp)
    assert disp == {%{hello: [], world: [{0, pid, ref, match_fun}]}, %{ref => :world}, 10, 10}

    {:ok, 0, disp} = D.ask(5, {pid, ref}, disp)
    assert disp == {%{hello: [], world: [{5, pid, ref, match_fun}]}, %{ref => :world}, 5, 10}

    {:ok, 0, disp} = D.cancel({pid, ref}, disp)
    assert disp == {%{hello: [], world: []}, %{}, 10, 10}
  end

  test "subscribes, asks and dispatches" do
    pid  = self()
    ref  = make_ref()
    disp = dispatcher([])
    match_fun = &true_fun/1

    {:ok, 0, disp} = D.subscribe([name: :test, func: match_fun], {pid, ref}, disp)

    {:ok, 3, disp} = D.ask(3, {pid, ref}, disp)
    assert disp == {%{:test => [{3, pid, ref, match_fun}]}, %{ref => :test}, 0, 3}

    {:ok, [], disp} = D.dispatch([:a], disp)
    assert disp == {%{:test => [{2, pid, ref, match_fun}]}, %{ref => :test}, 0, 3}
    assert_received {:"$gen_consumer", {_, ^ref}, [:a]}

    {:ok, 3, disp} = D.ask(3, {pid, ref}, disp)
    assert disp == {%{:test => [{5, pid, ref, match_fun}]}, %{ref => :test}, 0, 3}

    {:ok, [:g, :h], disp} = D.dispatch([:b, :c, :d, :e, :f, :g, :h], disp)
    assert disp == {%{:test => [{0, pid, ref, match_fun}]}, %{ref => :test}, 0, 3}
    assert_received {:"$gen_consumer", {_, ^ref}, [:b, :c, :d, :e, :f]}

    {:ok, [:i, :j], disp} = D.dispatch([:i, :j], disp)
    assert disp == {%{:test => [{0, pid, ref, match_fun}]}, %{ref => :test}, 0, 3}
    refute_received {:"$gen_consumer", {_, ^ref}, _}
  end

  test "subscribes, asks multiple consumers" do
    pid  = self()
    ref1 = make_ref()
    ref2 = make_ref()
    ref3 = make_ref()
    disp = dispatcher([])
    true_match_fun = &true_fun/1
    false_match_fun = &false_fun/1

    {:ok, 0, disp} = D.subscribe([name: :foo, func: true_match_fun], {pid, ref1}, disp)
    {:ok, 0, disp} = D.subscribe([name: :foo, func: false_match_fun], {pid, ref2}, disp)
    {:ok, 0, disp} = D.subscribe([name: :bar, func: true_match_fun], {pid, ref3}, disp)

    {:ok, 4, disp} = D.ask(4, {pid, ref1}, disp)
    {:ok, 2, disp} = D.ask(2, {pid, ref2}, disp)
    {:ok, 3, disp} = D.ask(3, {pid, ref3}, disp)
    assert disp == {%{:foo => [{4, pid, ref1, true_match_fun}, {2, pid, ref2, false_match_fun}], :bar => [{3, pid, ref3, true_match_fun}]}, %{ref1 => :foo, ref2 => :foo, ref3 => :bar}, 0, 4}

    {:ok, 2, disp} = D.ask(2, {pid, ref3}, disp)
    assert disp == {%{:foo => [{4, pid, ref1, true_match_fun}, {2, pid, ref2, false_match_fun}], :bar => [{5, pid, ref3, true_match_fun}]}, %{ref1 => :foo, ref2 => :foo, ref3 => :bar}, 0, 4}

    {:ok, 4, disp} = D.ask(4, {pid, ref2}, disp)
    assert disp == {%{:foo => [{6, pid, ref2, false_match_fun}, {4, pid, ref1, true_match_fun}], :bar => [{5, pid, ref3, true_match_fun}]}, %{ref1 => :foo, ref2 => :foo, ref3 => :bar}, 0, 4}
  end

  test "subscribes, asks and dispatches to multiple consumers" do
    pid  = self()
    ref1 = make_ref()
    ref2 = make_ref()
    disp = dispatcher([])
    true_match_fun = &true_fun/1
    abc_match_fun = fn e -> e in [:a, :b, :c] end

    {:ok, 0, disp} = D.subscribe([name: :foo, func: true_match_fun], {pid, ref1}, disp)
    {:ok, 0, disp} = D.subscribe([name: :bar, func: abc_match_fun], {pid, ref2}, disp)

    {:ok, 3, disp} = D.ask(3, {pid, ref1}, disp)
    {:ok, 2, disp} = D.ask(2, {pid, ref2}, disp)
    assert disp == {%{:foo => [{3, pid, ref1, true_match_fun}], :bar => [{2, pid, ref2, abc_match_fun}]}, %{ref1 => :foo, ref2 => :bar}, 0, 3}

    {:ok, [:c, :d, :e], disp} = D.dispatch([:a, :b, :c, :d, :e], disp)
    assert disp == {%{:foo => [{1, pid, ref1, true_match_fun}], :bar => [{0, pid, ref2, abc_match_fun}]}, %{ref1 => :foo, ref2 => :bar}, 0, 3}
    assert_received {:"$gen_consumer", {_, ^ref1}, [:a, :b]}
    assert_received {:"$gen_consumer", {_, ^ref2}, [:a, :b]}

    {:ok, [:a, :b, :c], disp} = D.dispatch([:a, :b, :c], disp)
    assert disp == {%{:foo => [{1, pid, ref1, true_match_fun}], :bar => [{0, pid, ref2, abc_match_fun}]}, %{ref1 => :foo, ref2 => :bar}, 0, 3}
    refute_received {:"$gen_consumer", {_, _}, _}

    {:ok, 3, disp} = D.ask(3, {pid, ref1}, disp)
    {:ok, 3, disp} = D.ask(3, {pid, ref2}, disp)
    assert disp == {%{:foo => [{4, pid, ref1, true_match_fun}], :bar => [{3, pid, ref2, abc_match_fun}]}, %{ref1 => :foo, ref2 => :bar}, 0, 3}

    {:ok, [], disp} = D.dispatch([:a, :b], disp)
    assert disp == {%{:foo => [{2, pid, ref1, true_match_fun}], :bar => [{1, pid, ref2, abc_match_fun}]}, %{ref1 => :foo, ref2 => :bar}, 0, 3}
    assert_received {:"$gen_consumer", {_, ^ref1}, [:a, :b]}
    assert_received {:"$gen_consumer", {_, ^ref2}, [:a, :b]}

    {:ok, [], disp} = D.dispatch([:c, :d], disp)
    assert disp == {%{:foo => [{0, pid, ref1, true_match_fun}], :bar => [{0, pid, ref2, abc_match_fun}]}, %{ref1 => :foo, ref2 => :bar}, 0, 3}
    assert_received {:"$gen_consumer", {_, ^ref1}, [:c, :d]}
    assert_received {:"$gen_consumer", {_, ^ref2}, [:c]}

    {:ok, 3, disp} = D.ask(3, {pid, ref1}, disp)
    {:ok, 2, disp} = D.ask(2, {pid, ref2}, disp)
    assert disp == {%{:foo => [{3, pid, ref1, true_match_fun}], :bar => [{2, pid, ref2, abc_match_fun}]}, %{ref1 => :foo, ref2 => :bar}, 0, 3}

    ref3 = make_ref()
    efg_match_fun = fn e -> e in [:e, :f, :g] end
    {:ok, 0, disp} = D.subscribe([name: :bar, func: efg_match_fun], {pid, ref3}, disp)

    {:ok, 3, disp} = D.ask(3, {pid, ref3}, disp)
    assert disp == {%{:foo => [{3, pid, ref1, true_match_fun}], :bar => [{3, pid, ref3, efg_match_fun}, {2, pid, ref2, abc_match_fun}]}, %{ref1 => :foo, ref2 => :bar, ref3 => :bar}, 0, 3}

    {:ok, [], disp} = D.dispatch([:e, :f, :g], disp)
    assert disp == {%{:foo => [{0, pid, ref1, true_match_fun}], :bar => [{2, pid, ref2, abc_match_fun}, {0, pid, ref3, efg_match_fun}]}, %{ref1 => :foo, ref2 => :bar, ref3 => :bar}, 0, 3}
    assert_received {:"$gen_consumer", {_, ^ref1}, [:e, :f, :g]}
    assert_received {:"$gen_consumer", {_, ^ref3}, [:e, :f, :g]}
    refute_received {:"$gen_consumer", {_, ^ref2}, _}
  end

  test "delivers notifications to all consumers" do
    pid  = self()
    ref1 = make_ref()
    ref2 = make_ref()
    disp = dispatcher([])
    true_match_fun = &true_fun/1
    false_match_fun = &false_fun/1

    {:ok, 0, disp} = D.subscribe([name: :foo, func: true_match_fun], {pid, ref1}, disp)
    {:ok, 0, disp} = D.subscribe([name: :bar, func: false_match_fun], {pid, ref2}, disp)
    {:ok, 3, disp} = D.ask(3, {pid, ref1}, disp)

    {:ok, notify_disp} = D.notify(:hello, disp)
    assert disp == notify_disp

    assert_received {:"$gen_consumer", {_, ^ref1}, {:notification, :hello}}
    assert_received {:"$gen_consumer", {_, ^ref2}, {:notification, :hello}}
  end

  test "warns on demand mismatch" do
    pid  = self()
    ref1 = make_ref()
    ref2 = make_ref()
    disp = dispatcher([])
    true_match_fun = &true_fun/1
    false_match_fun = &false_fun/1

    {:ok, 0, disp} = D.subscribe([name: :foo, func: true_match_fun], {pid, ref1}, disp)
    {:ok, 0, disp} = D.subscribe([name: :bar, func: false_match_fun], {pid, ref2}, disp)

    assert capture_log(fn ->
      {:ok, 3, disp} = D.ask(3, {pid, ref1}, disp)
      {:ok, 4, disp} = D.ask(4, {pid, ref2}, disp)
      disp
    end) =~ "GenStage producer DemandDispatcher expects a maximum demand of 3"
  end
end