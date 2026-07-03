defmodule Anubis.Server.Transport.StreamableHTTP.EventStore.InMemoryTest do
  use ExUnit.Case, async: true

  alias Anubis.Server.Transport.StreamableHTTP.EventStore.InMemory

  defp start_store(opts \\ []) do
    name = :"event_store_#{System.unique_integer([:positive])}"
    start_supervised!({InMemory, Keyword.put(opts, :name, name)})
    name
  end

  describe "append/3 and latest_id/2" do
    test "assigns monotonic, session-scoped ids starting at 1" do
      store = start_store()

      assert {:ok, 1} = InMemory.append(store, "s1", "a")
      assert {:ok, 2} = InMemory.append(store, "s1", "b")
      assert {:ok, 3} = InMemory.append(store, "s1", "c")

      assert {:ok, 3} = InMemory.latest_id(store, "s1")
    end

    test "counters are isolated per session" do
      store = start_store()

      assert {:ok, 1} = InMemory.append(store, "s1", "a")
      assert {:ok, 1} = InMemory.append(store, "s2", "a")
      assert {:ok, 2} = InMemory.append(store, "s1", "b")

      assert {:ok, 2} = InMemory.latest_id(store, "s1")
      assert {:ok, 1} = InMemory.latest_id(store, "s2")
    end

    test "latest_id is 0 for an unknown session" do
      store = start_store()
      assert {:ok, 0} = InMemory.latest_id(store, "never-seen")
    end
  end

  describe "replay/3" do
    test "returns events after the cursor in ascending id order" do
      store = start_store()
      for data <- ~w(a b c d), do: InMemory.append(store, "s1", data)

      assert {:ok, [{2, "b"}, {3, "c"}, {4, "d"}]} = InMemory.replay(store, "s1", 1)
    end

    test "returns an empty list when the cursor is current" do
      store = start_store()
      InMemory.append(store, "s1", "a")

      assert {:ok, []} = InMemory.replay(store, "s1", 1)
    end

    test "returns an empty list for an unknown session" do
      store = start_store()
      assert {:ok, []} = InMemory.replay(store, "unknown", 0)
    end

    test "cursor of 0 replays the whole retained ring" do
      store = start_store()
      InMemory.append(store, "s1", "a")
      InMemory.append(store, "s1", "b")

      assert {:ok, [{1, "a"}, {2, "b"}]} = InMemory.replay(store, "s1", 0)
    end
  end

  describe "bounded history (cursor-older-than-ring)" do
    test "retains only the most recent history_size events" do
      store = start_store(history_size: 3)
      for data <- ~w(a b c d e), do: InMemory.append(store, "s1", data)

      # ids 1 and 2 were evicted; only 3,4,5 remain.
      assert {:ok, [{3, "c"}, {4, "d"}, {5, "e"}]} = InMemory.replay(store, "s1", 0)
    end

    test "a cursor older than the ring replays only what is still retained" do
      store = start_store(history_size: 3)
      for data <- ~w(a b c d e), do: InMemory.append(store, "s1", data)

      # Client asks to resume from 1, but 2 was evicted: best-effort tail.
      assert {:ok, [{3, "c"}, {4, "d"}, {5, "e"}]} = InMemory.replay(store, "s1", 1)
    end

    test "ids keep advancing monotonically past evictions" do
      store = start_store(history_size: 2)
      for data <- ~w(a b c d), do: InMemory.append(store, "s1", data)

      assert {:ok, 4} = InMemory.latest_id(store, "s1")
      assert {:ok, 5} = InMemory.append(store, "s1", "e")
    end
  end

  describe "delete/2" do
    test "drops a session's recorded events and resets its counter" do
      store = start_store()
      InMemory.append(store, "s1", "a")
      InMemory.append(store, "s1", "b")

      assert :ok = InMemory.delete(store, "s1")

      assert {:ok, []} = InMemory.replay(store, "s1", 0)
      assert {:ok, 0} = InMemory.latest_id(store, "s1")
    end

    test "is idempotent for an unknown session" do
      store = start_store()
      assert :ok = InMemory.delete(store, "unknown")
    end
  end

  describe "bounded sessions" do
    test "evicts the least-recently-appended session past max_sessions" do
      store = start_store(max_sessions: 2)

      InMemory.append(store, "s1", "a")
      InMemory.append(store, "s2", "a")
      # s3 pushes the count to 3 > 2, evicting the oldest-touched session (s1).
      InMemory.append(store, "s3", "a")

      assert {:ok, 0} = InMemory.latest_id(store, "s1")
      assert {:ok, 1} = InMemory.latest_id(store, "s2")
      assert {:ok, 1} = InMemory.latest_id(store, "s3")
    end

    test "re-touching a session protects it from eviction" do
      store = start_store(max_sessions: 2)

      InMemory.append(store, "s1", "a")
      InMemory.append(store, "s2", "a")
      # Touch s1 so s2 becomes the least-recently-appended, then add s3.
      InMemory.append(store, "s1", "b")
      InMemory.append(store, "s3", "a")

      assert {:ok, 0} = InMemory.latest_id(store, "s2")
      assert {:ok, 2} = InMemory.latest_id(store, "s1")
      assert {:ok, 1} = InMemory.latest_id(store, "s3")
    end
  end

  describe "bound validation" do
    test "rejects a non-positive history_size" do
      assert_raise ArgumentError, fn -> InMemory.init(history_size: 0) end
      assert_raise ArgumentError, fn -> InMemory.init(history_size: -1) end
      assert_raise ArgumentError, fn -> InMemory.init(history_size: :lots) end
    end

    test "rejects a non-positive, non-:infinity max_sessions" do
      assert_raise ArgumentError, fn -> InMemory.init(max_sessions: 0) end
      assert_raise ArgumentError, fn -> InMemory.init(max_sessions: -5) end
    end

    test "accepts :infinity for max_sessions" do
      assert {:ok, %{max_sessions: :infinity}} = InMemory.init(max_sessions: :infinity)
    end
  end
end
