{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE RecordWildCards #-}

module Network.Wai.Handler.Warp.HTTP2.Worker where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception as E
import Control.Monad (void, forever, when)
import Data.Typeable
import qualified Network.HTTP.Types as H
import Network.HTTP2
import Network.HTTP2.Priority
import Network.Wai
import Network.Wai.Internal (Response(..))
import Network.Wai.Handler.Warp.HTTP2.Types
import Network.Wai.Handler.Warp.IORef
import qualified Network.Wai.Handler.Warp.Settings as S
import qualified Network.Wai.Handler.Warp.Response as R
import qualified Network.Wai.Handler.Warp.Timeout as T
import Network.Wai.Internal (ResponseReceived(..))

----------------------------------------------------------------

type Responder = Stream -> Priority -> Request -> Response -> IO ResponseReceived

response :: Context -> Responder
response Context{..} strm pri req rsp = do
    case rsp of
        ResponseStream _ _ strmbdy -> do
            -- We must not exit this WAI application.
            -- If the application exits, streaming would be also closed.
            -- Since 'StreamingBody' is loop, we cannot control it.
            -- So, let's serialize 'Builder' with a designated queue.
            sq <- newTBQueueIO 10
            tvar <- newTVarIO SyncNone
            enqueue outputQ (OResponse strm rsp (Persist sq tvar)) pri
            let push b = atomically $ writeTBQueue sq (SBuilder b)
                flush  = atomically $ writeTBQueue sq SFlush
            -- Since we must not enqueue an empty queue to the priority
            -- queue, we spawn a thread to ensure that the designated
            -- queue is not empty.
            void $ forkIO $ waiter tvar sq (enqueue outputQ) strm pri
            strmbdy push flush
            atomically $ writeTBQueue sq SFinish
        _ -> do
            let hasBody = requestMethod req /= H.methodHead
                       || R.hasBody (responseStatus rsp)
            enqueue outputQ (OResponse strm rsp (Oneshot hasBody)) pri
    return ResponseReceived

data Break = Break deriving (Show, Typeable)

instance Exception Break

-- fixme: sending a reset frame?
worker :: Context -> S.Settings -> T.Manager -> Application -> Responder -> IO ()
worker Context{..} set tm app responder = do
    tid <- myThreadId
    ref <- newIORef Nothing
    bracket (T.register tm (E.throwTo tid Break)) T.cancel $ \th ->
        go th ref `E.catch` gonext th ref
  where
    go th ref = forever $ do
        Input strm req pri <- atomically $ readTQueue inputQ
        T.tickle th
        writeIORef ref (Just strm)
        E.handle (S.settingsOnException set Nothing)
                 (void $ app req $ responder strm pri req)
    gonext th ref Break = doit `E.catch` gonext th ref
      where
        doit = do
            m <- readIORef ref
            case m of
                Nothing   -> return ()
                Just strm -> do
                    -- fixme: sending 500
                    writeIORef (streamState strm) $ Closed Killed
                    writeIORef ref Nothing
                    go th ref

waiter :: TVar Sync -> TBQueue Sequence
       -> (Output -> Priority -> IO ()) -> Stream -> Priority
       -> IO ()
waiter tvar sq enq strm pri = do
    mx <- atomically $ do
        mout <- readTVar tvar
        case mout of
            SyncNone     -> retry
            SyncNext nxt -> do
                writeTVar tvar SyncNone
                return $ Just nxt
            SyncFinish   -> return Nothing
    case mx of
        Nothing -> return ()
        Just next -> do
            atomically $ do
                isEmpty <- isEmptyTBQueue sq
                when isEmpty retry
            enq (ONext strm next) pri
            waiter tvar sq enq strm pri

