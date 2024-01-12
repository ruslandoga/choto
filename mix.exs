defmodule Choto.MixProject do
  use Mix.Project

  def project do
    [
      app: :choto,
      version: "0.1.0",
      elixir: "~> 1.14",
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    []
  end

  # Specifies which paths to compile per environment.
  # defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:castore, "~> 0.1.0 or ~> 1.0", optional: true},
      {:dialyxir, "~> 1.3", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.29", only: :dev},
      {:rexbug, "~> 1.0", only: [:dev, :test]},
      {:benchee, "~> 1.1", only: [:bench], runtime: false}
    ]
  end
end
