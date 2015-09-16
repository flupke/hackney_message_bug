defmodule HackneyMessageBugTest do
  use ExUnit.Case
  alias HackneyMessageBug.Download

  setup_all do
    {:ok, _} = :application.ensure_all_started(:httparrot)
    :ok
  end

  setup do
    temp_dir = create_temp_dir()

    on_exit fn ->
      File.rm_rf!(temp_dir)
    end

    {:ok, [temp_dir: temp_dir]}
  end

  defp create_temp_dir() do
    dirname = Path.join([System.tmp_dir, "stashex-" <> UUID.uuid4()])
    File.mkdir!(dirname)
    dirname
  end

  defp test_single_download(context) do
    url = "http://localhost:8080/ip"
    name = UUID.uuid4()
    body_filename = Path.join([context[:temp_dir], "#{name}-body"])
    meta_filename = Path.join([context[:temp_dir], "#{name}-meta"])
    {:ok, _filename} = Download.get(url, body_filename, meta_filename)
    assert Poison.decode!(File.read!(body_filename)) == %{"origin" => "127.0.0.1"}
  end

  defp do_ten_downloads(context) do
    1..10
    |> Enum.map(fn(_) -> Task.async(fn -> test_single_download(context) end) end)
    |> Enum.each(fn(task) -> Task.await(task, 10000) end)
  end

  test "concurrent downloads of the same URL", context do
    1..100000 |> Enum.each(fn(_) -> do_ten_downloads(context) end)
  end
end
