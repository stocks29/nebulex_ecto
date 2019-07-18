defmodule NebulexEcto.Repo do
  @moduledoc """
  Wrapper/Facade on top of `Nebulex.Cache` and `Ecto.Repo`.

  This module encapsulates the access to the Ecto repo and Nebulex cache,
  providing a set of functions compliant with the `Ecto.Repo` API.

  For retrieve-like functions, the wrapper access the cache first, if the
  requested data is found, then it is returned right away, otherwise, the
  wrapper tries to retrieve the data from the repo (database), and if the
  data is found, then it is cached so the next time it can be retrieved
  directly from cache.

  For write functions (insert, update, delete, ...), the wrapper runs the
  eviction logic, which can be delete the data from cache or just replace it;
  depending on the `:nbx_evict` option.

  When used, `NebulexEcto.Repo` expects the `:repo` and `:cache` as options.
  For example:

      defmodule MyApp.CacheableRepo do
        use NebulexEcto.Repo,
          cache: MyApp.Cache,
          repo: MyApp.Repo
      end

  The cache and repo respectively:

      defmodule MyApp.Cache do
        use Nebulex.Cache,
          otp_app: :my_app,
          adapter: Nebulex.Adapters.Local
      end

      defmodule MyApp.Repo do
        use Ecto.Repo,
          otp_app: :my_app,
          adapter: Ecto.Adapters.Postgres
      end

  And this is an example of how their configuration would looks like:

      config :my_app, MyApp.Cache,
        gc_interval: 3600

      config :my_app, MyApp.Repo,
        database: "ecto_simple",
        username: "postgres",
        password: "postgres",
        hostname: "localhost"

  ## Compile-time configuration options

    * `:cache` - a compile-time option that specifies the Nebulex cache
      to be used by the wrapper.

    * `:repo` - a compile-time option that specifies the Ecto repo
      to be used by the wrapper.

  To configure the cache and repo, see `Nebulex` and `Ecto` documentation
  respectively.

  ## Shared options

  Almost all of the operations below accept the following options:

    * `:nbx_key` - specifies the key to be used for cache access.
      By default is set to `{Ecto.Schema.t, id :: term}`, assuming
      the schema has a field `id` which is the primary key; if this
      is not your case, you must provide the `:nbx_key`.

    * `:nbx_evict` - specifies the eviction strategy, if it is set to
      `:delete` (the default), then the key is removed from cache, and
      if it is set to `:replace`, then the key is replaced with the
      new value into the cache.
  """

  @doc false
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      require Logger
      alias NebulexEcto.Repo, as: CacheableRepo

      {cache, repo} = CacheableRepo.compile_config(__MODULE__, opts)

      @cache cache
      @repo repo

      def __cache__, do: @cache

      def __repo__, do: @repo

      def all(queryable, opts \\ []) do
        do_all(queryable, opts, &@repo.all/2)
      end

      def get(queryable, id, opts \\ []) do
        do_get(queryable, id, opts, &@repo.get/3)
      end

      def get!(queryable, id, opts \\ []) do
        do_get(queryable, id, opts, &@repo.get!/3)
      end

      def get_by(queryable, clauses, opts \\ []) do
        do_get(queryable, clauses, opts, &@repo.get_by/3)
      end

      def get_by!(queryable, clauses, opts \\ []) do
        do_get(queryable, clauses, opts, &@repo.get_by!/3)
      end

      def insert(struct_or_changeset, opts \\ []) do
        execute(&@repo.insert/2, struct_or_changeset, opts)
      end

      def insert!(struct_or_changeset, opts \\ []) do
        execute!(&@repo.insert!/2, struct_or_changeset, opts)
      end

      def update(changeset, opts \\ []) do
        execute(&@repo.update/2, changeset, opts)
      end

      def update!(changeset, opts \\ []) do
        execute!(&@repo.update!/2, changeset, opts)
      end

      def delete(struct_or_changeset, opts \\ []) do
        execute(&@repo.delete/2, struct_or_changeset, Keyword.put(opts, :nbx_evict, :delete))
      end

      def delete!(struct_or_changeset, opts \\ []) do
        execute!(&@repo.delete!/2, struct_or_changeset, Keyword.put(opts, :nbx_evict, :delete))
      end

      def insert_or_update(struct_or_changeset, opts \\ []) do
        execute(&@repo.insert_or_update/2, struct_or_changeset, opts)
      end

      def insert_or_update!(struct_or_changeset, opts \\ []) do
        execute!(&@repo.insert_or_update!/2, struct_or_changeset, opts)
      end

      ## Helpers

      def key(%Ecto.Query{from: %{source: {_tablename, schema}}}, key),
        do: {schema, key}

      def key(%Ecto.Query{from: {_tablename, schema}}, key),
        do: {schema, key}

      def key(%{__struct__: struct}, key),
        do: {struct, key}

      def key(struct, key) when is_atom(struct),
        do: {struct, key}

      defp do_all(queryable, opts, fallback) do
        {nbx_key, opts} = Keyword.pop(opts, :nbx_key)
        {nbx_get_opts, opts} = Keyword.pop(opts, :nbx_get_opts, [])
        {nbx_set_opts, opts} = Keyword.pop(opts, :nbx_set_opts, [])
        cache_key = nbx_key || key(queryable, queryable)

        cond do
          value = @cache.get(cache_key, nbx_get_opts) ->
            value

          value = fallback.(queryable, opts) ->
            @cache.set(cache_key, value, nbx_set_opts)

          true ->
            nil
        end
      end

      defp do_get(queryable, key, opts, fallback) do
        {nbx_key, opts} = Keyword.pop(opts, :nbx_key)
        {nbx_get_opts, opts} = Keyword.pop(opts, :nbx_get_opts, [])
        {nbx_set_opts, opts} = Keyword.pop(opts, :nbx_set_opts, [])
        redir_key = nbx_key || key(queryable, key)

        cond do
          value = get_resolve(redir_key, nbx_get_opts) ->
            Logger.debug("fetched from from cache key=#{inspect(redir_key)} value=#{inspect(value)}")
            value

          value = fallback.(queryable, key, opts) ->
            cache_key = key(queryable, value.id)
            if redir_key != cache_key do
              value = {:redirect, cache_key}
              Logger.debug("setting cache key=#{inspect(redir_key)} value=#{inspect(value)}")
              @cache.set(redir_key, value, nbx_set_opts)
            end
            Logger.debug("setting cache key=#{inspect(cache_key)} value=#{inspect(value)}")
            @cache.set(cache_key, value, nbx_set_opts)

          true ->
            nil
        end
      end

      defp resolve_maybe_value({:redirect, cache_key}, opts) do
        @cache.get(cache_key, opts)
      end
      defp resolve_maybe_value(value, _opts) do
        value
      end

      defp get_resolve({:redirect, cache_key}, opts) do
        resolve_maybe_value(@cache.get(cache_key, opts), opts)
      end
      defp get_resolve(cache_key, opts) do
        resolve_maybe_value(@cache.get(cache_key, opts), opts)
      end

      defp execute(fun, struct_or_changeset, opts) do
        {nbx_key, opts} = Keyword.pop(opts, :nbx_key)
        {nbx_evict, opts} = Keyword.pop(opts, :nbx_evict, :delete)
        {nbx_opts, opts} = Keyword.pop(opts, :nbx_opts, [])

        case fun.(struct_or_changeset, opts) do
          {:ok, schema} = res ->
            cache_key = nbx_key || key(schema, schema.id)
            _ = cache_evict(nbx_evict, cache_key, schema, nbx_opts)
            res

          error ->
            error
        end
      end

      defp execute!(fun, struct_or_changeset, opts) do
        {nbx_key, opts} = Keyword.pop(opts, :nbx_key)
        {nbx_evict, opts} = Keyword.pop(opts, :nbx_evict, :delete)
        {nbx_opts, opts} = Keyword.pop(opts, :nbx_opts, [])

        schema = fun.(struct_or_changeset, opts)
        cache_key = nbx_key || key(schema, schema.id)
        _ = cache_evict(nbx_evict, cache_key, schema, nbx_opts)
        schema
      end

      defp cache_evict(:delete, key, _, opts),
        do: @cache.delete(key, opts)

      defp cache_evict(:replace, key, value, opts),
        do: @cache.set(key, value, opts)
    end
  end

  @doc """
  Retrieves the compile time configuration.
  """
  def compile_config(facade, opts) do
    unless cache = Keyword.get(opts, :cache) do
      raise ArgumentError, "missing :cache option in #{facade}"
    end

    unless repo = Keyword.get(opts, :repo) do
      raise ArgumentError, "missing :repo option in #{facade}"
    end

    {cache, repo}
  end
end
