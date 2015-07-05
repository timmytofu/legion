{-# LANGUAGE DeriveGeneric #-}
{- |
  Module contain `KeyState`, which is a wrapper around the opaque binary data
  that represents the user-defined state of a key.
-}
module Network.Legion.Data.KeyState (
  KeyState(..)
) where

import Data.Binary (Binary)
import Data.ByteString.Lazy (ByteString)
import GHC.Generics (Generic)

{- |
  This is the mutable state associated with a particular key. In a key/value
  system, this would be the value.
-}
newtype KeyState = KeyState {
    unstate :: ByteString
      -- ^ The user-defined value of the key state.
  }
  deriving (Show, Generic)

instance Binary KeyState


