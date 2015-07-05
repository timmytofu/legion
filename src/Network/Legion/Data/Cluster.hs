{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE DeriveGeneric #-}
{- |
  This module holds the cluster state data types.
-}

module Network.Legion.Data.Cluster (
  ClusterState(..),
  findKey,
  Peer,
  PeerMessage(..),
  PeerMessagePayload(..),
  Key(..),
  peerMsgSource
) where


import Control.Applicative ((<$>))
import Control.Concurrent (forkIO)
import Control.Concurrent.Chan (newChan)
import Control.Exception (throw, try, SomeException, catch)
import Control.Monad (void, forever, join)
import Control.Monad.Trans.Class (lift)
import Data.Binary (Binary(put, get))
import Data.ByteString.Lazy (ByteString)
import Data.Conduit (Source, ($=), ($$), yield)
import Data.Conduit.Network (sourceSocket)
import Data.Conduit.Serialization.Binary (conduitDecode)
import Data.DoubleWord (Word256(Word256), Word128(Word128))
import Data.Map (Map)
import Data.Word (Word8, Word64)
import GHC.Generics (Generic)
import Network.Legion.Data.AddressDescription (resolveAddr)
import Network.Legion.Data.KeyState (KeyState)
import Network.Legion.Data.LegionarySettings(LegionarySettings(
  LegionarySettings, peerBindAddr))
import Network.Legion.Data.Peer (Peer)
import Network.Legion.Utils (chanToSource, chanToSink)
import Network.Socket (Family(AF_INET, AF_INET6, AF_UNIX, AF_CAN),
  SocketOption(ReuseAddr), SocketType(Stream), accept, bindSocket,
  defaultProtocol, listen, setSocketOption, socket, SockAddr(SockAddrInet,
  SockAddrInet6, SockAddrUnix, SockAddrCan))
import qualified System.Log.Logger as L (warningM, errorM)


{- |
  ClusterState
-}
data ClusterState = ClusterState {
    peers :: Map Peer SockAddr,
    keyspace :: KeyTree,
    expectedAcks :: Map MessageId (IO ())
  }
  deriving (Generic)


{- |
  Find the location of a key.
-}
findKey :: Key -> KeyTree -> Peer
findKey _ P {peer} = peer
findKey key I {pivot, right, left}
  | key >= pivot = findKey key right
  | otherwise = findKey key left


{- |
  The type of keys.
-}
newtype Key = K {unkey :: Word256} deriving (Eq, Ord, Show)

instance Binary Key where
  put (K (Word256 (Word128 a b) (Word128 c d))) = put (a, b, c, d)
  get = do
    (a, b, c, d) <- get
    return (K (Word256 (Word128 a b) (Word128 c d)))


data KeyTree
  = I {
        pivot :: Key,
        left :: KeyTree,
        right :: KeyTree
      }
  | P {
        peer :: Peer
      }
  deriving (Eq)


type MessageId = Word64

{- |
  The type of messages sent to us from other peers.
-}
data PeerMessage = PeerMessage {
    source :: Peer,
    messageId :: MessageId,
    payload :: PeerMessagePayload
  }
  deriving (Generic, Show)

instance Binary PeerMessage


{- |
  The data contained within a peer message.

  TODO: Think about distinguishing the broadcast messages `NewPeer` and
  `Takeover`. Those messages are particularly important because they
  are the only ones for which it is necessary that every node eventually
  receive a copy of the message.

  When we get around to implementing durability and data replication,
  the sustained inability to confirm that a node has received one of
  these messages should result in the ejection of that node from the
  cluster and the blacklisting of that node so that it can never re-join.
-}
data PeerMessagePayload
  = StoreState Key KeyState
    -- ^ Tell the receiving node to store the key/state information in
    --   its persistence layer in preparation for a key range ownership
    --   handoff. The receiving node should NOT take ownership of this
    --   key, or start fielding user requests for this key, unless
    --   those requests are received via a `ForwardRequest` peer message
    --   originating from the current owner of the key.
  | StoreAck MessageId
    -- ^ Acknowledge the successful handling of a `StoreState` message.
  | NewPeer Peer BSockAddr
    -- ^ Tell the receiving node that a new peer has shown up in the
    --   cluster.  This message should initiate a handoff of some portion
    --   of the receiving node's keyspace to the new peer.
  | Handoff Key Key
    -- ^ Tell the receiving node that we would like it to take over the
    --   identified key range, which should have already been transmitted
    --   using a series of `StoreState` messages.
  | Takeover Key Key MessageId
    -- ^ Announce that the sending node is taking over the identified
    --   key range, in response to a handoff message (identified by the
    --   message id) sent by the original owner of that key range,
  | ForwardRequest ByteString
    -- ^ Forward a binary encoded user request to the receiving node.
  | ForwardResponse ByteString MessageId
    -- ^ Respond to the forwarded request, identified by MessageId,
    --   with the binary encoded user response.
  deriving (Generic, Show)

instance Binary PeerMessagePayload


{- |
  Construct a source of incoming peer messages.
-}
peerMsgSource :: LegionarySettings -> Source IO PeerMessage
peerMsgSource LegionarySettings {peerBindAddr} = join . lift $
    catch (do
        bindAddr <- resolveAddr peerBindAddr
        inputChan <- newChan
        so <- socket (fam bindAddr) Stream defaultProtocol
        setSocketOption so ReuseAddr 1
        bindSocket so bindAddr
        listen so 5
        (void . forkIO) $ acceptLoop so inputChan
        return (chanToSource inputChan)
      ) (\err -> do
        errorM
          $ "Couldn't start incomming peer message service, because of: "
          ++ show (err :: SomeException)
        -- the following is a cute trick to forward exceptions downstream
        -- using a thunk.
        (return . yield . throw) err
      )
  where
    acceptLoop so inputChan =
        catch (
          forever $ do
            (conn, _) <- accept so
            (void . forkIO . logErrors)
              (sourceSocket conn $= conduitDecode $$ msgSink)
        ) (\err -> do
          errorM $ "error in accept loop: " ++ show (err :: SomeException)
          yield (throw err) $$ msgSink
        )
      where
        msgSink = chanToSink inputChan
        logErrors io = do
          result <- try io
          case result of
            Left err ->
              warningM
                $ "Incomming peer connection crashed because of: "
                ++ show (err :: SomeException)
            Right v -> return v


{- |
  Guess the family of a `SockAddr`.
-}
fam :: SockAddr -> Family
fam SockAddrInet {} = AF_INET
fam SockAddrInet6 {} = AF_INET6
fam SockAddrUnix {} = AF_UNIX
fam SockAddrCan {} = AF_CAN


{- |
  Shorthand logging.
-}
warningM :: String -> IO ()
warningM = L.warningM "legion.peer"


{- |
  Shorthand logging.
-}
errorM :: String -> IO ()
errorM = L.errorM "legion.peer"


{- |
  A type useful only for creating a `Binary` instance of `SockAddr`.
-}
newtype BSockAddr = BSockAddr {getAddr :: SockAddr} deriving (Show)

instance Binary BSockAddr where
  put (BSockAddr addr) =
    case addr of
      SockAddrInet p h -> do
        put (0 :: Word8)
        put (fromEnum p, h)
      SockAddrInet6 p f h s -> do
        put (1 :: Word8)
        put (fromEnum p, f, h, s)
      SockAddrUnix s -> do
        put (2 :: Word8)
        put s
      SockAddrCan a -> do
        put (3 :: Word8)
        put a

  get = BSockAddr <$> do
    c <- get
    case (c :: Word8) of
      0 -> do
        (p, h) <- get
        return (SockAddrInet (toEnum p) h)
      1 -> do
        (p, f, h, s) <- get
        return (SockAddrInet6 (toEnum p) f h s)
      2 -> SockAddrUnix <$> get
      3 -> SockAddrCan <$> get
      _ ->
        fail
          $ "Can't decode BSockAddr because the constructor tag "
          ++ "was not understood. Probably this data is representing "
          ++ "something else."


