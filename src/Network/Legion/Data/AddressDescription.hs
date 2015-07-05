{- |
  This module parses strings into `SockAddr`s.
-}
module Network.Legion.Data.AddressDescription (
  AddressDescription,
  resolveAddr
) where

import Control.Applicative ((<$>))
import Data.List.Split (splitOn)
import Network.Socket (SockAddr, addrAddress, getAddrInfo)

type AddressDescription = String

{- |
  Resolve an address description into an actual socket addr.
-}
resolveAddr :: AddressDescription -> IO SockAddr
resolveAddr desc =
  case splitOn ":" desc of
    ["ipv4", name, port] ->
      addrAddress . head <$> getAddrInfo Nothing (Just name) (Just port)
    _ -> error ("Invalid address description: " ++ show desc)


