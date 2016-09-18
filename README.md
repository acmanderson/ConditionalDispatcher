# ConditionalDispatcher

GenStage dispatcher that delivers and filters events based on the events the consumers want. 

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed as:

  1. Add `conditional_dispatcher` to your list of dependencies in `mix.exs`:

    ```elixir
    def deps do
      [{:conditional_dispatcher, "~> 0.1.0"}]
    end
    ```

  2. Ensure `conditional_dispatcher` is started before your application:

    ```elixir
    def application do
      [applications: [:conditional_dispatcher]]
    end
    ```

