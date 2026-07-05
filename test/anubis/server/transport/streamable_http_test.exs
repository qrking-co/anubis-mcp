defmodule Anubis.Server.Transport.StreamableHTTPTest do
  use Anubis.MCP.Case, async: false

  import ExUnit.CaptureLog

  alias Anubis.Server.Registry
  alias Anubis.Server.Transport.StreamableHTTP

  @moduletag capture_log: true

  describe "start_link/1" do
    test "starts with valid options" do
      server = :"test_server_#{System.unique_integer([:positive])}"
      name = Registry.transport_name(server, :streamable_http)
      sup = Registry.task_supervisor_name(server)

      assert {:ok, pid} =
               StreamableHTTP.start_link(server: server, name: name, task_supervisor: sup)

      assert Process.alive?(pid)
    end

    test "requires server option" do
      assert_raise Peri.InvalidSchema, fn ->
        StreamableHTTP.start_link(name: :test)
      end
    end
  end

  describe "with running transport" do
    setup do
      name = Registry.transport_name(StubServer, :streamable_http)
      sup = Registry.task_supervisor_name(StubServer)
      start_supervised!({Task.Supervisor, name: sup})

      {:ok, transport} =
        start_supervised({StreamableHTTP, server: StubServer, name: name, task_supervisor: sup})

      %{transport: transport, server: StubServer}
    end

    test "registers and unregisters SSE handlers", %{transport: transport} do
      session_id = "test-session-123"
      handler_pid = self()

      assert :ok = StreamableHTTP.register_sse_handler(transport, session_id)
      assert ^handler_pid = StreamableHTTP.get_sse_handler(transport, session_id)
      assert :ok = StreamableHTTP.unregister_sse_handler(transport, session_id)
      refute StreamableHTTP.get_sse_handler(transport, session_id)
    end

    test "register_sse_handler/3 stores subscriber metadata", %{transport: transport} do
      session_id = "metadata-session"
      handler_pid = self()

      subscriber = %{
        session_id: session_id,
        handler_pid: handler_pid,
        project: "forge-symphony",
        operator_role: "implementer"
      }

      assert :ok = StreamableHTTP.register_sse_handler(transport, session_id, subscriber)

      state = :sys.get_state(transport)

      assert %{
               session_id: ^session_id,
               handler_pid: ^handler_pid,
               project: "forge-symphony",
               operator_role: "implementer",
               registered_at: %DateTime{}
             } = Map.fetch!(state.sse_handlers, {session_id, handler_pid})
    end

    test "register_sse_handler/2 stores compatibility subscriber metadata", %{
      transport: transport
    } do
      session_id = "compat-session"
      handler_pid = self()

      assert :ok = StreamableHTTP.register_sse_handler(transport, session_id)

      state = :sys.get_state(transport)

      assert %{
               session_id: ^session_id,
               handler_pid: ^handler_pid,
               project: nil,
               operator_role: nil,
               registered_at: %DateTime{}
             } = Map.fetch!(state.sse_handlers, {session_id, handler_pid})
    end

    test "unregister_sse_handler/3 removes by session and handler pid", %{
      transport: transport
    } do
      session_id = "unregister-session"
      handler_pid = self()

      assert :ok =
               StreamableHTTP.register_sse_handler(transport, session_id, %{
                 session_id: session_id,
                 handler_pid: handler_pid,
                 project: "forge-symphony",
                 operator_role: "operator"
               })

      assert :ok = StreamableHTTP.unregister_sse_handler(transport, session_id, handler_pid)
      state = :sys.get_state(transport)

      refute Map.has_key?(state.sse_handlers, {session_id, handler_pid})
    end

    test "handler_count/1 returns total and handler_count/2 scopes by project", %{
      transport: transport
    } do
      test_pid = self()

      first_handler =
        spawn(fn ->
          :ok =
            StreamableHTTP.register_sse_handler(transport, "project-a-session", %{
              session_id: "project-a-session",
              handler_pid: self(),
              project: "project-a",
              operator_role: "operator"
            })

          send(test_pid, {:registered, self()})

          receive do
            :stop -> :ok
          end
        end)

      second_handler =
        spawn(fn ->
          :ok =
            StreamableHTTP.register_sse_handler(transport, "project-b-session", %{
              session_id: "project-b-session",
              handler_pid: self(),
              project: "project-b",
              operator_role: "operator"
            })

          send(test_pid, {:registered, self()})

          receive do
            :stop -> :ok
          end
        end)

      assert_receive {:registered, ^first_handler}
      assert_receive {:registered, ^second_handler}

      assert StreamableHTTP.handler_count(transport) == 2
      assert StreamableHTTP.handler_count(transport, {:project, "project-a"}) == 1
      assert StreamableHTTP.handler_count(transport, {:project, "project-b"}) == 1

      send(first_handler, :stop)
      send(second_handler, :stop)
    end

    test "compatibility-registered handlers are excluded from project-scoped counts", %{
      transport: transport
    } do
      assert :ok = StreamableHTTP.register_sse_handler(transport, "compat-count-session")

      assert StreamableHTTP.handler_count(transport) == 1
      assert StreamableHTTP.handler_count(transport, {:project, "forge-symphony"}) == 0
    end

    test "stale unregister cannot remove a newer handler", %{transport: transport} do
      session_id = "test-session-race"
      test_pid = self()

      old_handler =
        spawn(fn ->
          :ok = StreamableHTTP.register_sse_handler(transport, session_id)
          send(test_pid, {:registered, self()})

          receive do
            :stop -> :ok
          end
        end)

      assert_receive {:registered, ^old_handler}

      new_handler =
        spawn(fn ->
          :ok = StreamableHTTP.register_sse_handler(transport, session_id)
          send(test_pid, {:registered, self()})

          receive do
            :stop -> :ok
          end
        end)

      assert_receive {:registered, ^new_handler}
      assert ^new_handler = StreamableHTTP.get_sse_handler(transport, session_id)

      # Simulate delayed close from old SSE connection.
      assert :ok = StreamableHTTP.unregister_sse_handler(transport, session_id, old_handler)
      assert ^new_handler = StreamableHTTP.get_sse_handler(transport, session_id)

      assert :ok = StreamableHTTP.unregister_sse_handler(transport, session_id, new_handler)
      refute StreamableHTTP.get_sse_handler(transport, session_id)

      send(old_handler, :stop)
      send(new_handler, :stop)
    end

    test "a superseded handler is not proactively closed", %{transport: transport} do
      session_id = "test-session-supersede"
      test_pid = self()

      old_handler =
        spawn(fn ->
          :ok = StreamableHTTP.register_sse_handler(transport, session_id)
          send(test_pid, {:registered, self()})

          receive do
            :close_sse -> send(test_pid, {:closed, self()})
          end
        end)

      assert_receive {:registered, ^old_handler}

      # A second connection takes over the same session.
      new_handler =
        spawn(fn ->
          :ok = StreamableHTTP.register_sse_handler(transport, session_id)
          send(test_pid, {:registered, self()})

          receive do
            :stop -> :ok
          end
        end)

      assert_receive {:registered, ^new_handler}

      # The new handler becomes the active one for the session...
      assert ^new_handler = StreamableHTTP.get_sse_handler(transport, session_id)

      # ...and the superseded handler is NOT sent :close_sse. A server-initiated
      # close would make a spec-compliant client immediately reconnect, racing
      # the next registration into an unbounded register/close flap.
      refute_receive {:closed, ^old_handler}, 200

      send(old_handler, :close_sse)
      send(new_handler, :stop)
    end

    test "routes messages to sessions", %{transport: transport} do
      session_id = "test-session-789"

      assert :ok = StreamableHTTP.register_sse_handler(transport, session_id)

      message = "test message"
      assert :ok = StreamableHTTP.route_to_session(transport, session_id, message)

      assert_receive {:sse_message, ^message}

      capture_log(fn ->
        StreamableHTTP.unregister_sse_handler(transport, session_id)
        Process.sleep(10)
      end)
    end

    test "send_message_to_session/4 delivers only to the target session", %{
      transport: transport
    } do
      test_pid = self()

      session_a =
        spawn(fn ->
          :ok = StreamableHTTP.register_sse_handler(transport, "session-a")
          send(test_pid, {:registered, "session-a", self()})

          receive do
            {:sse_message, message} -> send(test_pid, {:delivered, "session-a", message})
            :stop -> :ok
          end
        end)

      session_b =
        spawn(fn ->
          :ok = StreamableHTTP.register_sse_handler(transport, "session-b")
          send(test_pid, {:registered, "session-b", self()})

          receive do
            {:sse_message, message} -> send(test_pid, {:delivered, "session-b", message})
            :stop -> :ok
          end
        end)

      assert_receive {:registered, "session-a", ^session_a}
      assert_receive {:registered, "session-b", ^session_b}

      assert :ok =
               StreamableHTTP.send_message_to_session(
                 transport,
                 "session-a",
                 "targeted-session",
                 timeout: 5000
               )

      assert_receive {:delivered, "session-a", "targeted-session"}
      refute_receive {:delivered, "session-b", "targeted-session"}, 100

      send(session_a, :stop)
      send(session_b, :stop)
    end

    test "send_message_to_project/4 delivers only to matching project subscribers", %{
      transport: transport
    } do
      test_pid = self()

      project_a =
        spawn(fn ->
          :ok =
            StreamableHTTP.register_sse_handler(transport, "project-a-session", %{
              session_id: "project-a-session",
              handler_pid: self(),
              project: "project-a",
              operator_role: "operator"
            })

          send(test_pid, {:registered, "project-a", self()})

          receive do
            {:sse_message, message} -> send(test_pid, {:delivered, "project-a", message})
            :stop -> :ok
          end
        end)

      project_b =
        spawn(fn ->
          :ok =
            StreamableHTTP.register_sse_handler(transport, "project-b-session", %{
              session_id: "project-b-session",
              handler_pid: self(),
              project: "project-b",
              operator_role: "operator"
            })

          send(test_pid, {:registered, "project-b", self()})

          receive do
            {:sse_message, message} -> send(test_pid, {:delivered, "project-b", message})
            :stop -> :ok
          end
        end)

      assert :ok = StreamableHTTP.register_sse_handler(transport, "compat-project-session")

      assert_receive {:registered, "project-a", ^project_a}
      assert_receive {:registered, "project-b", ^project_b}

      assert :ok =
               StreamableHTTP.send_message_to_project(
                 transport,
                 "project-a",
                 "targeted-project",
                 timeout: 5000
               )

      assert_receive {:delivered, "project-a", "targeted-project"}
      refute_receive {:delivered, "project-b", "targeted-project"}, 100
      refute_receive {:sse_message, "targeted-project"}, 100

      send(project_a, :stop)
      send(project_b, :stop)
    end

    test "send_message_to_subscribers/4 accepts subscriber selectors", %{
      transport: transport
    } do
      assert :ok =
               StreamableHTTP.send_message_to_subscribers(
                 transport,
                 fn subscriber -> Map.get(subscriber, :operator_role) == "operator" end,
                 "selector-message",
                 timeout: 5000
               )
    end

    test "cleans up handlers when they crash", %{transport: transport} do
      session_id = "test-session-crash"
      test_pid = self()

      capture_log(fn ->
        handler_pid =
          spawn(fn ->
            StreamableHTTP.register_sse_handler(transport, session_id)
            send(test_pid, :registered)

            receive do
              :crash -> exit(:boom)
            end
          end)

        assert_receive :registered, 1000

        handler = StreamableHTTP.get_sse_handler(transport, session_id)
        assert is_pid(handler)

        send(handler_pid, :crash)
        Process.sleep(100)

        refute StreamableHTTP.get_sse_handler(transport, session_id)
      end)
    end

    test "send_message/3 works", %{transport: transport} do
      message = "test message"
      assert :ok = StreamableHTTP.send_message(transport, message, timeout: 5000)
    end

    test "shutdown/1 gracefully shuts down", %{transport: transport} do
      assert Process.alive?(transport)
      assert :ok = StreamableHTTP.shutdown(transport)
      Process.sleep(100)
      refute Process.alive?(transport)
    end
  end

  describe "supported_protocol_versions/0" do
    test "returns supported versions" do
      versions = StreamableHTTP.supported_protocol_versions()
      assert is_list(versions)
      assert "2025-03-26" in versions
    end
  end
end
