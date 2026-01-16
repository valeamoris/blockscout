defmodule Explorer.Chain.CsvExport.Addresses do
  @moduledoc """
  Exports addresses list to a csv file.
  """

  alias Ecto.Association.NotLoaded
  alias Explorer.Chain.Address
  alias Explorer.Chain.Address.{Name, Reputation}
  alias Explorer.Chain.CsvExport.Helper

  @spec export(Keyword.t()) :: Enumerable.t()
  def export(options) do
    paging_options = Keyword.get(options, :paging_options, Helper.paging_options())
    sorting_options = Keyword.get(options, :sorting, [])

    full_options =
      [
        paging_options: paging_options,
        sorting: sorting_options,
        api?: true,
        necessity_by_association: %{
          :names => :optional,
          :smart_contract => :optional,
          :scam_badge => :optional,
          :token => :optional
        }
      ]

    addresses = Address.list_top_addresses(full_options)

    # Manually preload reputation since it's an embedded schema and can't be preloaded via join_associations
    addresses_with_reputation = preload_reputation(addresses)

    addresses_with_reputation
    |> to_csv_format()
    |> Helper.dump_to_stream()
  end

  defp preload_reputation(addresses) do
    address_hashes = Enum.map(addresses, & &1.hash)

    hash_to_reputation =
      address_hashes
      |> Reputation.preload_reputation()
      |> Map.new()

    Enum.map(addresses, fn address ->
      reputation = Map.get(hash_to_reputation, address.hash)
      %{address | reputation: reputation}
    end)
  end

  defp to_csv_format(addresses) do
    row_names = [
      "Address Hash",
      "Coin Balance",
      "Transactions Count",
      "Is Contract",
      "Name",
      "Is Scam",
      "Reputation"
    ]

    address_lists =
      addresses
      |> Stream.map(fn address ->
        [
          Address.checksum(address.hash),
          format_coin_balance(address.fetched_coin_balance),
          format_transactions_count(address.transactions_count),
          Address.smart_contract?(address),
          address_name(address),
          address_marked_as_scam?(address),
          address_reputation(address)
        ]
      end)

    Stream.concat([row_names], address_lists)
  end

  defp format_coin_balance(nil), do: "0"
  defp format_coin_balance(%{value: value}) when is_nil(value), do: "0"
  defp format_coin_balance(%{value: value}), do: to_string(value)

  defp format_transactions_count(nil), do: "0"
  defp format_transactions_count(count), do: to_string(count)

  defp address_name(%Address{names: names}) when is_list(names) and length(names) > 0 do
    case Enum.find(names, &(&1.primary == true)) do
      nil ->
        # take last created address name, if there is no `primary` one.
        %Name{name: name} = Enum.max_by(names, & &1.id)
        name

      %Name{name: name} ->
        name
    end
  end

  defp address_name(_), do: nil

  defp address_marked_as_scam?(%Address{scam_badge: %NotLoaded{}}), do: false
  defp address_marked_as_scam?(%Address{scam_badge: scam_badge}) when not is_nil(scam_badge), do: true
  defp address_marked_as_scam?(_), do: false

  # Reputation logic matches Helper.address_with_info: if scam_badge exists, return "scam", otherwise check reputation
  defp address_reputation(%Address{scam_badge: scam_badge}) when not is_nil(scam_badge) do
    "scam"
  end

  defp address_reputation(address) do
    address_reputation_if_loaded(address)
  end

  defp address_reputation_if_loaded(%Address{reputation: %Reputation{reputation: reputation}}) do
    reputation
  end

  defp address_reputation_if_loaded(_) do
    "ok"
  end
end
