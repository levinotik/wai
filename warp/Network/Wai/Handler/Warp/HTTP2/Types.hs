{-# LANGUAGE OverloadedStrings, CPP, RecordWildCards #-}

module Network.Wai.Handler.Warp.HTTP2.Types where

#if __GLASGOW_HASKELL__ < 709
import Control.Applicative ((<$>),(<*>))
#endif
import Blaze.ByteString.Builder (Builder)
import Control.Concurrent
import Control.Concurrent.STM
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder.Extra as B
import Data.IntMap.Strict (IntMap)
import qualified Data.IntMap.Strict as M
import qualified Network.HTTP.Types as H
import Network.Wai (Request, Response)
import Network.Wai.Handler.Warp.IORef
import Network.Wai.Handler.Warp.Types

import Network.HTTP2
import Network.HTTP2.Priority
import Network.HPACK

----------------------------------------------------------------

http2ver :: H.HttpVersion
http2ver = H.HttpVersion 2 0

isHTTP2 :: Transport -> Bool
isHTTP2 TCP = False
isHTTP2 tls = useHTTP2
  where
    useHTTP2 = case tlsNegotiatedProtocol tls of
        Nothing    -> False
        Just proto -> "h2-" `BS.isPrefixOf` proto

----------------------------------------------------------------

data Input = Input Stream Request Priority

----------------------------------------------------------------

data Control a = CFinish
               | CNext a
               | CNone

type DynaNext = WindowSize -> IO Next

type BytesFilled = Int

data Next = Next BytesFilled (Control DynaNext)

data Output = OFinish
            | OGoaway ByteString
            | OFrame  ByteString
            | OResponse Stream Response Aux
            | ONext Stream DynaNext

----------------------------------------------------------------

data Sequence = SFinish
              | SFlush
              | SBuilder Builder

data Sync = SyncNone
          | SyncFinish
          | SyncNext DynaNext

data Aux = Oneshot Bool
         | Persist (TBQueue Sequence) (TVar Sync)

data Leftover = LZero
              | LOne B.BufferWriter
              | LTwo ByteString B.BufferWriter

----------------------------------------------------------------

type StreamTable = IntMap Stream

data Context = Context {
    http2settings      :: IORef Settings
  , streamTable        :: IORef StreamTable
  , concurrency        :: IORef Int
  , continued          :: IORef (Maybe StreamId)
  , currentStreamId    :: IORef StreamId
  , inputQ             :: TQueue Input
  , outputQ            :: PriorityTree Output
  , encodeDynamicTable :: IORef DynamicTable
  , decodeDynamicTable :: IORef DynamicTable
  , wait               :: MVar ()
  , connectionWindow   :: TVar WindowSize
  }

----------------------------------------------------------------

newContext :: IO Context
newContext = Context <$> newIORef defaultSettings
                     <*> newIORef M.empty
                     <*> newIORef 0
                     <*> newIORef Nothing
                     <*> newIORef 0
                     <*> newTQueueIO
                     <*> newPriorityTree
                     <*> (newDynamicTableForEncoding defaultDynamicTableSize >>= newIORef)
                     <*> (newDynamicTableForDecoding defaultDynamicTableSize >>= newIORef)
                     <*> newEmptyMVar
                     <*> newTVarIO defaultInitialWindowSize

----------------------------------------------------------------

data ClosedCode = Finished | Killed | Reset ErrorCodeId deriving Show

data StreamState =
    Idle
  | Continued [HeaderBlockFragment]
              Int  -- Total size
              Int  -- The number of continuation frames
              Bool -- End of stream
              Priority
  | NoBody HeaderList Priority
  | HasBody HeaderList Priority
  | Body (TQueue ByteString)
  | HalfClosed
  | Closed ClosedCode

instance Show StreamState where
    show Idle        = "Idle"
    show Continued{} = "Continued"
    show NoBody{}    = "NoBody"
    show HasBody{}   = "HasBody"
    show Body{}      = "Body"
    show HalfClosed  = "HalfClosed"
    show (Closed e)  = "Closed: " ++ show e

----------------------------------------------------------------

data Stream = Stream {
    streamNumber        :: StreamId
  , streamState         :: IORef StreamState
  -- Next two fields are for error checking.
  , streamContentLength :: IORef (Maybe Int)
  , streamBodyLength    :: IORef Int
  , streamWindow        :: TVar WindowSize
  }

newStream :: StreamId -> WindowSize -> IO Stream
newStream sid win = Stream sid <$> newIORef Idle
                               <*> newIORef Nothing
                               <*> newIORef 0
                               <*> newTVarIO win
