{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE DeriveGeneric #-}
{- |
  Legion is a framework designed to help people implement large-scale
  distributed services that function using a value-space partitioning
  strategy, sometimes known as "sharding". Examples of services that
  rely on value-space partitioning include ElasticSearch, Riak, DynamoDB,
  and others.

  In other words, this framework is an abstraction over partitioning,
  cluster-rebalancing,  node discovery, and request routing, allowing
  the user to focus on request logic and storage strategies.

  In its current alpha state, this framework does not provide data
  replication, but future milestones do include that goal.
-}

module Network.Legion (
  -- * Invoking Legionary
  -- $invocation
  runLegionary,
  forkLegionary,
  -- * Service Implementation
  -- $service-implementaiton
  Legionary(..),
  Persistence(..),
  RequestMsg,
  -- * Fundamental Types
  PartitionKey(..),
  PartitionState(..),
  -- * Framework Configuration
  -- $framework-config
  LegionarySettings(..),
  AddressDescription,
  DiscoverySettings(..),
  ConfigDiscovery,
  MulticastDiscovery(..),
  CustomDiscovery(..),
  -- * Utils
  newMemoryPersistence,
  diskPersistence
) where

import Prelude hiding (lookup, null, mapM, readFile, writeFile)

import Control.Applicative ((<$>))
import Control.Concurrent (forkIO)
import Control.Concurrent.Chan (Chan, newChan, writeChan, readChan)
import Control.Concurrent.MVar (newEmptyMVar, takeMVar, putMVar)
import Control.Concurrent.STM (atomically)
import Control.Concurrent.STM.TVar (newTVar, modifyTVar, readTVar)
import Control.Exception (throw, try, SomeException, catch)
import Control.Monad (void, forever, join, (>=>))
import Control.Monad.Trans.Class (MonadTrans, lift)
import Data.Binary (Binary(put, get), encode, decode)
import Data.Bool (bool)
import Data.ByteString.Lazy (ByteString, readFile, writeFile, toStrict,
  fromStrict)
import Data.Conduit (Source, Sink, ($$), await, ($=), yield, await, leftover)
import Data.Conduit.List (sourceList)
import Data.Conduit.Network (sourceSocket)
import Data.Conduit.Serialization.Binary (conduitDecode)
import Data.DoubleWord (Word256(Word256), Word128(Word128))
import Data.HexString (hexString, fromBytes, toBytes)
import Data.List.Split (splitOn)
import Data.Map (Map, empty, insert, delete, lookup, singleton, lookupLE)
import Data.Maybe (fromJust, fromMaybe)
import Data.Text (Text)
import Data.UUID.V1 (nextUUID)
import Data.Word (Word8, Word64)
import GHC.Generics (Generic)
import Network.Socket (Family(AF_INET, AF_INET6, AF_UNIX, AF_CAN),
  SocketOption(ReuseAddr), SocketType(Stream), accept, bindSocket,
  defaultProtocol, listen, setSocketOption, socket, SockAddr(SockAddrInet,
  SockAddrInet6, SockAddrUnix, SockAddrCan), addrAddress, getAddrInfo,
  close, connect)
import Network.Socket.ByteString.Lazy (sendAll)
import System.Directory (removeFile, doesFileExist, getDirectoryContents)
import qualified Data.ByteString.Char8 as B (pack)
import qualified Data.Conduit.List as CL (map)
import qualified Data.HexString as Hex (toText)
import qualified Data.Text as T (unpack)
import qualified Data.UUID as UUID (toText)
import qualified System.Log.Logger as L (debugM, warningM, errorM)

-- $invocation
-- Notes on invocation.

{- |
  Run the legion node framework program, with the given user definitions,
  framework settings, and request source. This function never returns
  (except maybe with an exception if something goes horribly wrong).

  For the vast majority of service implementations, you are going to need
  to implement some halfway complex concurrency in order to populate the
  request source, and to handle the responses. Unless you know exactly
  what you are doing, you probably want to use `forkLegionary` instead.
-}
runLegionary :: (Binary response, Binary request)
  => Legionary request response
    -- ^ The user-defined legion application to run.
  -> LegionarySettings
    -- ^ Settings and configuration of the legionary framework.
  -> Source IO (RequestMsg request response)
    -- ^ A source of requests, together with a way to respond to the requets.
  -> IO ()
runLegionary legionary settings requestSource = do
    nodeState <- initNodeState
    (peerMsgSource settings `merge` requestSource)
      $$ requestSink legionary nodeState
  where
    initNodeState =
        maybe (Left settings) Right <$> readNodeState (persistence legionary)
      where
        readNodeState = error "readNodeState undefined"
        

{- |
  Build a brand new node state, for the first node in a cluster.
-}
makeNewFirstNode :: (MonadTrans t) => LegionarySettings -> t IO NodeState
makeNewFirstNode settings = lift $ do
  bindAddr <- resolveAddr (peerBindAddr settings)
  self <- UUID.toText . fromJust <$> nextUUID
  return NodeState {
      peers = singleton self (BSockAddr bindAddr),
      keyspace = singleton minBound self,
      self
    }


{- |
  Forks the legion framework in a background thread, and returns a way to
  send user requests to it and retrieve the responses to those requests.
-}
forkLegionary :: (Binary response, Binary request)
  => Legionary request response
    -- ^ The user-defined legion application to run.
  -> LegionarySettings
    -- ^ Settings and configuration of the legionary framework.
  -> IO (PartitionKey -> request -> IO response)
forkLegionary legionary settings = do
  chan <- newChan
  (void . forkIO) (runLegionary legionary settings (chanToSource chan))
  return (\ key request -> do
      responseVar <- newEmptyMVar
      writeChan chan ((key, request), putMVar responseVar)
      takeMVar responseVar
    )


-- $service-implementaiton
-- The only thing required to implement a legion service is to provide a
-- request handler and a persistence layer. Both of these things have the
-- obvious semantics. The request handler is a function that transforms
-- a request and a possibly non-existent partition state into a response
-- and a new, possibly non-existent, partition state.
-- 
-- The persistence layer provides the framework with a way to store
-- the various partition states. This allows you to choose any number
-- of persistence strategies, including only in memory, on disk, or in
-- some external database.
--
-- See `newMemoryPersistence` and `diskPersistence` if you need to get
-- started quickly with an in-memory persistence layer.


{- |
  This is the type of a user-defined Legion application. Implement this and
  allow the Legion framework to manage your cluster.
-}
data Legionary request response = Legionary {
    {- |
      The request handler, implemented by the user to service requests.

      Returns a response to the request, together with the new partitoin
      state.
    -}
    handleRequest
      :: PartitionKey
      -> request
      -> Maybe PartitionState
      -> (response, Maybe PartitionState),
    {- |
      The user-defined persistence layer implementation.
    -}
    persistence :: Persistence
  }


{- |
  The type of a user-defined persistence strategy used to persist
  partition states. See `newMemoryPersistence` or `diskPersistence`
  if you need to get started quicky.
-}
data Persistence = Persistence {
    getState :: PartitionKey -> IO (Maybe PartitionState),
    saveState :: PartitionKey -> PartitionState -> IO (),
    deleteState :: PartitionKey -> IO (),
    listKeys :: Source IO PartitionKey
  }


{- |
  This is how requests are packaged when they are sent to the legion framework
  for handling. It includes the request information itself, a partition key to
  which the request is directed, and a way for the framework to deliver the
  response to some interested party.

  Unless you know exactly what you are doing, you will have used
  `forkLegionary` instead of `runLegionary` to run the framework, in
  which case you can safely ignore the existence of this type.
-}
type RequestMsg request response = ((PartitionKey, request), response -> IO ())


{- |
  This is how partitions are identified and referenced.
-}
newtype PartitionKey = K {unkey :: Word256} deriving (Eq, Ord, Show, Bounded)

instance Binary PartitionKey where
  put (K (Word256 (Word128 a b) (Word128 c d))) = put (a, b, c, d)
  get = do
    (a, b, c, d) <- get
    return (K (Word256 (Word128 a b) (Word128 c d)))


{- |
  This is the mutable state associated with a particular key. In a key/value
  system, this would be the value.
  
  The partition state is represented as an opaque byte string, and it
  is up to the service implementation to make sure that the binary data
  is encoded and decoded into whatever form the service needs.
-}
newtype PartitionState = PartitionState {
    unstate :: ByteString
  }
  deriving (Show, Generic)

instance Binary PartitionState


-- $framework-config
-- The legion framework has several operational parameters which can
-- be controlled using configuration. These include the address binding
-- used to expose the cluster management service endpoint, and details
-- about the automatic node discovery strategy.
--
-- The node discovery strategy dictates how the legion framework goes
-- about finding other nodes on the network with which it can form a
-- cluster. UDP multicast works well if you are running your own hardware,
-- but it is not supported by most cloud environments like Amazon's EC2.


{- |
  Settings used when starting up the legion framework.
-}
data LegionarySettings = LegionarySettings {
    peerBindAddr :: AddressDescription,
      -- ^ The address on which the legion framework will listen for
      --   rebalancing and cluster management commands.
    discovery :: DiscoverySettings,
    stateFile :: FilePath
  }


{- |
  An address description is really just an synonym for a formatted string.

  The only currently supported address address family is: @ipv4@

  Examples: @"ipv4:0.0.0.0:8080"@, @"ipv4:www.google.com:80"@,
-}
type AddressDescription = String


{- |
  Configuration of how to discover peers.
-}
data DiscoverySettings
  = Config ConfigDiscovery
  | Multicast MulticastDiscovery
  | Custom CustomDiscovery


{- |
  Peer discovery based on static configuration.
-}
type ConfigDiscovery = Map Peer AddressDescription


{- |
  Not implemented yet.
-}
data MulticastDiscovery = MulticastDiscovery {} 


{- |
  Not implemented yet. This is a way to allow users to provide their
  own discovery mechanisms. For instance, in EC2 (which disallows udp
  traffic) some people use third party service discovery tools like
  Consul or Eureka.  Integrating with all of these tools is beyond
  the scope of this package, but some integrations may be provided by
  supplemental packages such as @legion-consul@ or @legion-eureka@.
-}
data CustomDiscovery = CustomDiscovery {}


{- |
  A convenient memory-based persistence layer. Good for testing or for
  applications (like caches) that don't have durability requirements.
-}
newMemoryPersistence :: IO Persistence
newMemoryPersistence = do
    cacheT <- atomically (newTVar empty)
    return Persistence {
        getState = fetchState cacheT,
        saveState = (.) (atomically . modifyTVar cacheT) . insert,
        deleteState = atomically . modifyTVar cacheT . delete,
        listKeys = return ()
      }
  where
    fetchState cacheT key = atomically $
      lookup key <$> readTVar cacheT


{- |
  A convenient way to persist partition states to disk.
-}
diskPersistence
  :: FilePath
    -- ^ The directory under which partition states will be stored.
  -> Persistence
diskPersistence directory = Persistence {
      getState,
      saveState,
      deleteState,
      listKeys
    }
  where
    getState key =
      let path = toPath key in
      doesFileExist path >>= bool
        ((Just . PartitionState) <$> readFile path)
        (return Nothing)

    saveState key state = writeFile (toPath key) (unstate state)

    deleteState = removeFile . toPath

    listKeys = do
      keys <- lift $ fmap fromHex <$> getDirectoryContents directory
      sourceList keys

    {- |
      Convert a key to a path
    -}
    toPath :: PartitionKey -> FilePath
    toPath key = directory ++ "/" ++ toHex key

    {- |
      Convert a partition key to a hexidecimal string.
    -}
    toHex :: PartitionKey -> String
    toHex = T.unpack . Hex.toText . fromBytes . toStrict . encode
    
    fromHex :: String -> PartitionKey
    fromHex = decode . fromStrict . toBytes . hexString . B.pack


handlePeerMessage :: (Binary response, Binary request)
  => Legionary request response
  -> NodeState
  -> Map MessageId (response -> IO ())
  -> ConnectionManager
  -> PeerMessage
  -> IO NodeState

handlePeerMessage -- StoreState
    Legionary {persistence}
    nodeState
    _
    cm
    msg@PeerMessage {source, messageId, payload = StoreState key state}
  = do
    debugM ("Received StoreState: " ++ show msg)
    saveState persistence key state
    void $ send cm source (StoreAck messageId)
    return nodeState

handlePeerMessage -- NewPeer
    Legionary {}
    nodeState@NodeState {peers}
    _
    cm
    msg@PeerMessage {payload = NewPeer peer addy}
  = do
    debugM ("Received NewPeer: " ++ show msg)
    -- add the peer to the list
    let newPeers = insert peer addy peers
    updatePeers cm newPeers
    kickoffRebalancing
    return nodeState {peers = newPeers}
  where
    kickoffRebalancing = void $ error "kickoffRebalancing undefined"

handlePeerMessage -- ForwardRequest
    Legionary {handleRequest, persistence}
    nodeState
    _
    cm
    msg@PeerMessage {
        payload = ForwardRequest key request,
        source,
        messageId
      }
  = do
    let respond = void . send cm source . ForwardResponse messageId . encode
    debugM ("Received ForwardRequest: " ++ show msg)
    
    -- TODO 
    --   - figure out some slick concurrency here, by maintaining
    --       a map of keys that are currently being accessed or
    --       something
    --   - partitioning, balancing, etc.
    -- 
    either (respond . rethrow) respond =<< try (do 
        state <- getState persistence key
        let (response, newState) = handleRequest key (decode request) state
        updateState newState
        return response
      )
    return nodeState
  where
    {- |
      rethrow is just a reification of `throw`.
    -}
    rethrow :: SomeException -> a
    rethrow = throw
    updateState Nothing =
      deleteState persistence key
    updateState (Just state) =
      saveState persistence key state

handlePeerMessage -- ForwardResponse
    Legionary {}
    nodeState
    forwarded
    _cm
    msg@PeerMessage {
        payload = ForwardResponse messageId response
      }
  = do
    case lookup messageId forwarded of
      Nothing -> warningM
        $  "This peer received a response for a forwarded request that it "
        ++ "didn't send. The only time you might expect to see this is if "
        ++ "this peer recently crashed and was immediately restarted. If "
        ++ "you are seeing this in other circumstances then probably "
        ++ "something is very wrong at the network level. The message was: "
        ++ show msg
      Just respond ->
        respond (decode response)
    return nodeState


{- |
  Merge two sources into one source. This is a concurrency abstraction.
  The resulting source will produce items from either of the input sources
  as they become available. As you would expect from a multi-producer,
  single-consumer concurrency abstraction, the ordering of items produced
  by each source is consistent relative to other items produced by
  that same source, but the interleaving of items from both sources
  is nondeterministic.
-}
merge :: Source IO a -> Source IO b -> Source IO (Either a b)
merge left right = do
  chan <- lift newChan
  (lift . void . forkIO) (left $= CL.map Left $$ chanToSink chan)
  (lift . void . forkIO) (right $= CL.map Right $$ chanToSink chan)
  chanToSource chan


{- |
  Shorthand logging.
-}
debugM :: String -> IO ()
debugM = L.debugM "legion"


{- |
  Resolve an address description into an actual socket addr.
-}
resolveAddr :: AddressDescription -> IO SockAddr
resolveAddr desc =
  case splitOn ":" desc of
    ["ipv4", name, port] ->
      addrAddress . head <$> getAddrInfo Nothing (Just name) (Just port)
    _ -> error ("Invalid address description: " ++ show desc)


{- |
  Defines the local state of a node in the cluster.
-}
data NodeState = NodeState {
    peers :: Map Peer BSockAddr,
    keyspace :: Map PartitionKey Peer,
    self :: Peer
  }


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
  = StoreState PartitionKey PartitionState
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
  | Handoff PartitionKey PartitionKey
    -- ^ Tell the receiving node that we would like it to take over the
    --   identified key range, which should have already been transmitted
    --   using a series of `StoreState` messages.
  | Takeover PartitionKey PartitionKey MessageId
    -- ^ Announce that the sending node is taking over the identified
    --   key range, in response to a handoff message (identified by the
    --   message id) sent by the original owner of that key range,
  | ForwardRequest PartitionKey ByteString
    -- ^ Forward a binary encoded user request to the receiving node.
  | ForwardResponse MessageId ByteString
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
warningM = L.warningM "legion"


{- |
  Shorthand logging.
-}
errorM :: String -> IO ()
errorM = L.errorM "legion"


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


{- |
  The way to identify a peer.
-}
type Peer = Text


{- |
  Convert a chanel into a Source.
-}
chanToSource :: Chan a -> Source IO a
chanToSource chan = forever $ yield =<< lift (readChan chan)


{- |
 Convert an chanel into a Sink.
-}
chanToSink :: Chan a -> Sink a IO ()
chanToSink chan = do
  val <- await
  case val of
    Nothing -> return ()
    Just v -> do
      lift (writeChan chan v)
      chanToSink chan


{- |
  This `Sink` is what actually handles all peer messages and user input.
-}
requestSink :: (Binary response, Binary request)
  => Legionary request response
  -> Either LegionarySettings NodeState
  -> Sink (Either PeerMessage (RequestMsg request response)) IO ()
requestSink legionary (Right nodeState) =
  requestSink2 legionary nodeState empty =<< initConnectionManager nodeState
requestSink legionary (Left settings) = do
    maybeMsg <- await
    case maybeMsg of
      Nothing -> return ()
      Just msg -> do
        nodeState <- case msg of
          Left _peerMsg ->
            -- The first message is a peer message, join the cluster
            -- and continue.
            joinCluster
          Right _userMsg ->
            -- The first message is a user message, create a new cluster.
            makeNewFirstNode settings
        -- push the message back on the queue and move forward.
        leftover msg
        cm <- initConnectionManager nodeState
        requestSink2 legionary nodeState empty cm
  where
    joinCluster = error "joinCluster undefined"


{- |
  This is the second stage of request handling, after the virgin node
  state has been established
-}
requestSink2 :: (Binary response, Binary request)
  => Legionary request response
  -> NodeState
  -> Map MessageId (response -> IO ())
  -> ConnectionManager
  -> Sink (Either PeerMessage (RequestMsg request response)) IO ()
requestSink2 l nodeState forwarded cm = do
    msg <- await
    case msg of
      Just (Left peerMsg) -> do
        newNodeState <-
          lift $ handlePeerMessage l nodeState forwarded cm peerMsg
        requestSink2 l newNodeState forwarded cm
      Just (Right ((key, request), respond)) -> do
        let peer = lookupPeer key (keyspace nodeState)
        mid <- lift $ send cm peer (ForwardRequest key (encode request))
        requestSink2 l nodeState (insert mid respond forwarded) cm
      Nothing ->
        return ()
  where
    lookupPeer a b = snd $ fromMaybe
      (error
        $ "Keyspace corrupt because it is missing a minimum value. "
        ++ "This is a bug"
      )
      (lookupLE a b)


{- |
  Initialize the connection manager based on the node state.
-}
initConnectionManager :: (MonadTrans t) => NodeState -> t IO ConnectionManager
initConnectionManager NodeState {self, peers} = lift $ do
    cmChan <- newChan
    -- FIXME `nextId = minBound` here is not sufficient!! We are
    -- not allowed to ever re-use a message id or else we risk data
    -- corruption. This is a relatively low probability bug so I'm
    -- punting for now until I figure out how I want to fix it. Probably
    -- by making message id be a combination of a startup-generated uuid
    -- and a number, or something like that.
    let cmState = CMState {cmPeers = peers, nextId = minBound}
    (void . forkIO . void) (runManager cmChan cmState)
    return ConnectionManager {cmChan}
  where
    {- |
      This is the function that implements the actual connection manager.
    -}
    runManager :: Chan CMMessage -> CMState -> IO CMState
    runManager chan = (foldr1 (>=>) . repeat) (
        \s@CMState {cmPeers, nextId} -> do
          msg <- readChan chan
          case msg of
            Send peer payload respond -> do
              -- respond with the message id as soon as we know it,
              -- which is immediately
              respond nextId
              case getAddr <$> lookup peer cmPeers of
                Nothing -> logNoPeer peer payload cmPeers
                Just addy -> do
                  -- send a message the hard way
                  -- TODO: reuse socket connections.
                  so <- socket (fam addy) Stream defaultProtocol
                  connect so addy
                  sendAll so (encode PeerMessage {
                      source = self,
                      messageId = nextId,
                      payload
                    })
                  close so
              return s {nextId = succ nextId}
            Peers newPeers ->
              return s {cmPeers = newPeers}
      )

    logNoPeer peer msg peers_ = errorM
      $ "Trying to send a message to the unknown peer " ++ show peer
      ++ ". Not sure how this can happen. Our internal peer table must "
      ++ "have gotten corrupted somehow. This is a bug. We are dropping "
      ++ "the message on the floor and continuing as if nothing happened. "
      ++ "The message payload was: " ++ show msg ++ ". The peer table is: "
      ++ show peers_


{- |
  A value of this type provides a handle to a connection manager
  instances.
-}
data ConnectionManager =
  ConnectionManager {
    cmChan :: Chan CMMessage
  }


{- |
  This is the internal state of the connection manager.
-}
data CMState =
  CMState {
    cmPeers :: Map Peer BSockAddr,
    nextId :: MessageId
  }


{- |
  This is the type of message understood by the connection manager.
-}
data CMMessage
  = Send Peer PeerMessagePayload (MessageId -> IO ())
  | Peers (Map Peer BSockAddr)


{- |
  Sends a peer message using the connection manager. Returns the messageId
  of the sent message.
-}
send :: ConnectionManager -> Peer -> PeerMessagePayload -> IO MessageId
send cm peer payload = do
  mvar <- newEmptyMVar
  writeChan (cmChan cm) (Send peer payload (putMVar mvar))
  takeMVar mvar


{- |
  Update the connection manager with some new peer information.
-}
updatePeers :: ConnectionManager -> Map Peer BSockAddr -> IO()
updatePeers cm peerMap = writeChan (cmChan cm) (Peers peerMap)


-- {- |
--   Does a lookup of a key in a map, and also removes that key from the map.
-- -}
-- lookupDelete :: (Ord k) => k -> Map k a -> (Maybe a, Map k a)
-- lookupDelete = updateLookupWithKey ((const . const) Nothing)


