defmodule HackneyMessageBug.Download do
  @moduledoc """
  High level HTTPoison wrapper to download files to disk.
  """
  @tmp_ext ".tmp"

  @doc """
  Download `url` in `filename`.

  Metadata such as download URL and headers is saved as json in `metadata_url`.

  Do nothing if `filename` already exists.

  Options:

  * `:connect_timeout` - timeout used when establishing a connection, in
    milliseconds (default: 10_000);

  * `:recv_timeout` - timeout used when receiving from a connection, in
    milliseconds (default: 30_000);

  * `:max_retries` - retry on connection and timeouts this number of times
    (default 0, no retries);

  * `:backoff_factor` - a backoff factor to apply between attempts, in
    milliseconds (default 1000). The formula to calculate time between attempts
    is:

        backoff_factor * (2 ^ (number_of_tries - 1))

  * `:backoff_max` - maximum backoff time, in milliseconds (default 30_000);

  * `:max_redirects` - maximum number of redirects to automatically follow;

  * `:retry_func` - a function determining if another attempt should be made
    in case of error. It should return `true` if another attempt should be
    made. It takes the number of tries (1-based) and the `options` passed to
    this function. Here is the default implementation:

        fn(tries, options) ->
          if tries < options[:max_retries] do
            backoff = round(options[:backoff_factor] * :math.pow(2, tries - 1))
            backoff = :erlang.min(backoff, options[:backoff_max])
            :timer.sleep(backoff)
            true
          end
        end

  * `:source_url` - save this instead of `url` in the metadata file;

  Return values:

  * `{:ok, filename}` - the file was downloaded and is available at `filename`;

  * `{:error, reason}` - there was a problem downloading the file
  """
  def get(url, filename, metadata_filename, options \\ [])
  def get("data:" <> _rest = url, filename, metadata_filename, options) do
    options = build_options(options)
    save_datauri(url, filename, metadata_filename, options)
  end
  def get(url, filename, metadata_filename, options) do
    options = build_options(options)
    save_http(url, filename, metadata_filename, options)
  end

  defp build_options(options) do
    defaults = [
      connect_timeout: 10_000,
      recv_timeout: 30_000,
      max_retries: 1,
      backoff_factor: 1000,
      backoff_max: 30_000,
      max_redirects: 10,
      retry_func: &default_retry_func/2,
      source_url: nil,
    ]
    Keyword.merge(defaults, options)
  end

  defp save_datauri(url, filename, metadata_filename, _options) do
    case ExDataURI.parse(url) do
      {:ok, mediatype, payload} ->
        payload_size = byte_size(payload)
        metadata = %{
          "url" => "data_uri",
          "download_url" => "data_uri",
          "headers" => %{
            "content-length" => payload_size,
            "content-type" => mediatype,
          },
          "status_code" => 200,
          "size" => payload_size
        }
        temp_filename = filename <> @tmp_ext
        temp_metadata_filename = metadata_filename <> @tmp_ext
        File.write!(temp_metadata_filename, Poison.encode!(metadata))
        File.write!(temp_filename, payload)
        :ok = :file.rename(temp_metadata_filename, metadata_filename)
        :ok = :file.rename(temp_filename, filename)
        {:ok, filename}
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp save_http(url,
                 filename,
                 metadata_filename,
                 options,
                 tries \\ 1,
                 redirects \\ 0) do
    temp_filename = filename <> @tmp_ext
    temp_metadata_filename = metadata_filename <> @tmp_ext
    case download(url, temp_filename, temp_metadata_filename, options) do
      :ok ->
        :ok = :file.rename(temp_metadata_filename, metadata_filename)
        :ok = :file.rename(temp_filename, filename)
        {:ok, filename}
      {:redirect, to} ->
        if redirects < options[:max_redirects] do
          save_http(to, filename, metadata_filename, options, tries, redirects + 1)
        else
          {:error, "too many redirects"}
        end
      {:error, reason} ->
        if options[:retry_func].(tries, options) do
          save_http(url, filename, metadata_filename, options, tries + 1, redirects)
        else
          case reason do
            %HTTPoison.Error{} = error ->
              {:error, "#{url} download failed after #{tries} tries: #{HTTPoison.Error.message(error)}"}
            reason ->
              {:error, "#{url} download failed after #{tries} tries: #{reason}"}
          end
        end
    end
  end

  defp download(url, filename, metadata_filename, options) do
    File.open! filename, [:write], fn(file) ->
      hackney_options = [
        follow_redirect: true,
        recv_timeout: options[:recv_timeout],
        connect_timeout: options[:connect_timeout],
        insecure: true,
      ]
      conf = Application.get_env(:stashex, Stashex.Cache)
      user_agent = conf[:user_agent]
      case HTTPoison.get(url, [], stream_to: self, hackney: hackney_options) do
        {:ok, %HTTPoison.AsyncResponse{id: response_id}} ->
          if options[:source_url] == nil do
            metadata_url = url
          else
            metadata_url = options[:source_url]
          end
          metadata = %{
            url: metadata_url,
            download_url: url,
            size: 0,
            headers: %{},
            status_code: nil,
          }
          case write_response(url, response_id, file, options, metadata) do
            {:ok, metadata} ->
              File.write!(metadata_filename, Poison.encode!(metadata))
              :ok
            ret -> ret
          end
        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp write_response(url, response_id, file, options, metadata) do
    receive do
      %HTTPoison.AsyncStatus{id: ^response_id, code: status_code} ->
        metadata = put_in(metadata.status_code, status_code)
        write_response(url, response_id, file, options, metadata)

      %HTTPoison.AsyncHeaders{id: ^response_id, headers: headers} ->
        # Poison don't like lists of 2-tuples, we need to convert headers to a
        # map
        headers = Enum.into(headers, %{})
        metadata = put_in(metadata.headers, headers)
        write_response(url, response_id, file, options, metadata)

      %HTTPoison.AsyncChunk{id: ^response_id, chunk: {:redirect, to, _headers}} ->
        redirect_url = absolute_url(to, url)
        {:redirect, redirect_url}

      %HTTPoison.AsyncChunk{id: ^response_id, chunk: chunk} ->
        :ok = IO.binwrite(file, chunk)
        metadata = %{metadata | size: metadata.size + byte_size(chunk)}
        write_response(url, response_id, file, options, metadata)

      %HTTPoison.AsyncEnd{id: ^response_id} ->
        {:ok, metadata}

      %HTTPoison.Error{id: ^response_id, reason: reason} ->
        {:error, "#{inspect reason}"}

      message ->
        {:error, "unexpected message received: #{inspect message}"}
    end
  end

  defp default_retry_func(tries, options) do
    if tries < options[:max_retries] do
      backoff = round(options[:backoff_factor] * :math.pow(2, tries - 1))
      backoff = :erlang.min(backoff, options[:backoff_max])
      :timer.sleep(backoff)
      true
    end
  end

  defp absolute_url("http://" <> _rest = url, _from), do: url
  defp absolute_url("https://" <> _rest = url, _from), do: url
  defp absolute_url(path, from) do
    path = case path do
      "/" <> _rest -> path
      _ -> "/" <> path
    end
    comps = URI.parse(from)
    if comps.query do
      query_str = "?#{comps.query}"
    else
      query_str = ""
    end
    if comps.fragment do
      fragment_str = "##{comps.fragment}"
    else
      fragment_str = ""
    end
    "#{comps.scheme}://#{comps.host}:#{comps.port}#{path}#{query_str}#{fragment_str}"
  end
end
