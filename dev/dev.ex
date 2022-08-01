defmodule Dev do
  require Logger

  def run do
    # Rexbug.start(["Choto :: return", "Choto.Decoder :: return"], msgs: 10000)
    {:ok, conn} = Choto.connect({127, 0, 0, 1}, 9000)

    try do
      :ok = Choto.query(conn, "select * from my_first_table")
      {:ok, packets, conn} = Choto.await(conn)
    catch
      kind, reason ->
        Logger.error(Exception.format(kind, reason, __STACKTRACE__))
        {kind, reason}
    after
      # Rexbug.stop()
      Choto.close(conn)
    end
  end
end
