{-# LANGUAGE NamedFieldPuns #-}
{- |
  Nothing withstands the Legion.
-}
module Network.Legion (
  runLegionary,
  forkLegionary,
  Legionary(..),
  Persistence(..),
  RequestMsg,
  newMemoryPersistence
) where

import Prelude hiding (lookup)

import Control.Applicative ((<$>))
import Control.Concurrent (forkIO)
import Control.Concurrent.Chan (newChan, writeChan)
import Control.Concurrent.MVar (newEmptyMVar, takeMVar, putMVar)
import Control.Concurrent.STM (atomically)
import Control.Concurrent.STM.TVar (newTVar, modifyTVar, readTVar)
import Control.Exception (try, throw, SomeException)
import Control.Monad (void)
import Control.Monad.Trans.Class (lift)
import Data.Conduit (Source, Sink, ($$), await, ($=))
import Data.Map (empty, insert, delete, lookup)
import Network.Legion.Data.Cluster (Key, PeerMessage(PeerMessage, source,
  messageId, payload), PeerMessagePayload(StoreState, StoreAck, NewPeer),
  peerMsgSource, ClusterState)
import Network.Legion.Data.KeyState (KeyState)
import Network.Legion.Data.LegionarySettings (LegionarySettings)
import Network.Legion.Utils (chanToSource, chanToSink)
import qualified Data.Conduit.List as CL (map)
import qualified System.Log.Logger as L (debugM)


{- |
  Forks a legionary process in a background thread, and returns a way to send
  user requests to it.
-}
forkLegionary
  :: Legionary request response
  -> LegionarySettings
  -> IO (Key -> request -> IO response)
forkLegionary legionary settings = do
  chan <- newChan
  (void . forkIO) (runLegionary legionary settings (chanToSource chan))
  return (\ key request -> do
      responseVar <- newEmptyMVar
      writeChan chan ((key, request), putMVar responseVar)
      takeMVar responseVar
    )


{- |
  Launch a legion node.
-}
runLegionary
  :: Legionary request response
    -- ^ The user-defined legion application to run.
  -> LegionarySettings
    -- ^ Settings and configuration of the legionary framework.
  -> Source IO (RequestMsg request response)
    -- ^ A source of requests, together with a way to respond to the requets.
  -> IO ()
runLegionary legionary settings requestSource = do
    cs <- initClusterState settings
    (peerMsgSource settings `merge` requestSource) $$ requestSink legionary cs
  where
    initClusterState = undefined


{- |
  This `Sink` is what actually handles all peer messages and user input.
-}
requestSink
  :: Legionary request response
  -> ClusterState
  -> Sink (Either PeerMessage (RequestMsg request response)) IO ()
requestSink l@Legionary {handleRequest, persistence} cs = do
    msg <- await
    case msg of
      Just (Left peerMsg) -> do
        newCs <- lift $ handlePeerMessage l cs peerMsg
        requestSink l newCs
      Just (Right ((key, request), respond)) -> do
        -- TODO 
        --   - figure out some slick concurrency here, by maintaining
        --       a map of keys that are currently being accessed or
        --       something
        --   - partitioning, balancing, etc.
        -- 
        lift $ either (respond . rethrow) respond =<< try (do 
            state <- getState persistence key
            (response, newState) <- handleRequest key request state
            updateState key newState
            return response
          )
        requestSink l cs
      Nothing ->
        return ()
  where
    {- |
      rethrow is just a reification of `throw`.
    -}
    rethrow :: SomeException -> a
    rethrow = throw
    updateState key Nothing =
      deleteState persistence key
    updateState key (Just state) =
      saveState persistence key state


handlePeerMessage
  :: Legionary request response
  -> ClusterState
  -> PeerMessage
  -> IO ClusterState

handlePeerMessage -- StoreState
    Legionary {persistence}
    cs
    msg@PeerMessage {source, messageId, payload = StoreState key state}
  = do
    debugM ("Received StoreState: " ++ show msg)
    saveState persistence key state
    void $ send source (StoreAck messageId)
    return cs
  where
    send = undefined

handlePeerMessage -- Ack
    Legionary {}
    cs
    msg@PeerMessage {payload = StoreAck _}
  = do
    debugM ("Received Ack: " ++ show msg)
    void undefined
    return cs

handlePeerMessage -- NewPeer
    Legionary {}
    cs
    msg@PeerMessage {payload = NewPeer _ _}
  = do
    debugM ("Received NewPeer: " ++ show msg)
    void undefined
    return cs


{- |
  This is the type of request that the user of this framework must reify.
-}
type RequestMsg request response = ((Key, request), response -> IO ())


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
  This is the type of a user-defined Legion application. Implement this and
  allow the Legion framework to manage your cluster.
-}
data Legionary request response = Legionary {
    {- |
      The request handler, implemented by the user to service requests.

      Returns a response to the request, together with the new state
      value of the key.
    -}
    handleRequest
      :: Key
      -> request
      -> Maybe KeyState
      -> IO (response, Maybe KeyState),
    {- |
      The user-defined persistence layer implementation.
    -}
    persistence :: Persistence
  }


{- |
  The type of a user-defined persistence strategy used to persist key state.
-}
data Persistence = Persistence {
    getState :: Key -> IO (Maybe KeyState),
    saveState :: Key -> KeyState -> IO (),
    deleteState :: Key -> IO ()
  }


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
        deleteState = atomically . modifyTVar cacheT . delete
      }
  where
    fetchState cacheT key = atomically $
      lookup key <$> readTVar cacheT


{- |
  Shorthand logging.
-}
debugM :: String -> IO ()
debugM = L.debugM "legion"


