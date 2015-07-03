
{- |
  Provides some handy functions that probably need to be re-organized
  into something different that "utils".
-}
module Network.Legion.Utils (
  chanToSource,
  chanToSink,
) where


import Control.Concurrent.Chan (Chan, writeChan, readChan)
import Control.Monad (forever)
import Control.Monad.Trans.Class (lift)
import Data.Conduit (Source, Sink, await, yield)


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


