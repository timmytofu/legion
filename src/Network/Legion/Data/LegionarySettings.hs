{-# LANGUAGE OverloadedStrings #-}
{- |
  Legionary configuration data.
-}
module Network.Legion.Data.LegionarySettings (
  LegionarySettings(..),
  DiscoverySettings(..),
  ConfigDiscovery,
  MulticastDiscovery(..),
  EurekaDiscovery(..)
) where

import Prelude hiding (mapM)

import Data.Map (Map)
import Network.Legion.Data.AddressDescription (AddressDescription)
import Network.Legion.Data.Peer (Peer)


{- |
  Settings used when starting up the legion framework.
-}
data LegionarySettings = LegionarySettings {
    peerBindAddr :: AddressDescription,
      -- ^ The address on which the legion framework will listen for
      --   rebalancing and cluster management commands.
    discovery :: DiscoverySettings
  }


{- |
  Configuration of how to discover peers.
-}
data DiscoverySettings
  = Config ConfigDiscovery
  | Multicast MulticastDiscovery
  | Eureka EurekaDiscovery


{- |
  Peer discovery based on static configuration.
-}
type ConfigDiscovery = Map Peer AddressDescription


data MulticastDiscovery = MulticastDiscovery {} 


data EurekaDiscovery = EurekaDiscovery {}


