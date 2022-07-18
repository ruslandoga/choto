defmodule Choto.MixProject do
  use Mix.Project

  def project do
    [
      app: :choto,
      version: "0.1.0",
      # TODO?
      elixir: "~> 1.12",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(:dev), do: ["lib", "dev"]
  defp elixirc_paths(_env), do: ["lib"]

  defp deps do
    [
      {:nimble_lz4, "~> 0.1.2"},
      {:rexbug, "~> 1.0", only: [:dev, :test]},
      {:benchee, "~> 1.1", only: [:bench], runtime: false}
    ]
  end
end
