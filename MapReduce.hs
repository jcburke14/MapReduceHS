{-# LANGUAGE MultiParamTypeClasses, RankNTypes, ExistentialQuantification, TemplateHaskell #-}

module MapReduce (
  makeSource,
  makeSink,
  makeMapper,
  makeReducer,
  MapReduceJob (..),
  Worker (..),
  TupleSource,
  TupleSink,
  WorkFlow (..),
  InPort,
  OutPort,
  runMapReduce,
  runWorker,
  __remoteTable
) where 

import Network.Transport hiding (send)
import qualified Data.ConfigFile as Conf
import Data.Monoid
import Data.Either.Utils
import Data.Binary
import Data.Map hiding (map)
import Data.ByteString.Lazy hiding (length,map,zip,hPutStr,concat,hGetContents,pack,empty,take)
import Data.ByteString.Lazy.Char8 (pack)
import Data.Digest.Pure.SHA
import System.Cmd
import System.Exit
import System.IO
import System.Environment
import Control.Monad.Error
import Control.Distributed.Process
import Control.Distributed.Process.Node hiding (newLocalNode)
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Serializable
import Control.Distributed.Process.Backend.SimpleLocalnet
import Control.Distributed.Backend.P2P (makeNodeId)
import Control.Distributed.Process.Internal.Types

data MapReduceJob k1 v1 k2 v2 = MapReduce {
  file :: FilePath, 
  reader :: TupleSource k1 v1,
  writer :: TupleSink k2 v2,
  workflow :: WorkFlow k1 v1 k2 v2,
  endDict :: StatDict k2 v2
}

data Worker k1 v1 k2 v2 = (Serializable k1, Serializable k2, Serializable v1, Serializable v2) => 
  Worker { 
    workClosure :: WorkClosure k1 v1 k2 v2,
    sdict :: StatDict k1 v1 
  }

type TupleSource k v = ([OutPort k v] -> Closure (ReceivePort String -> Process ()))

type TupleSink k v = (FilePath -> Closure (InPort k v -> Process ()))

type WorkClosure k1 v1 k2 v2 = [OutPort k2 v2] -> Closure (InPort k1 v1 -> Process ())

type OutPort k v = SendPort (Maybe (k,v))

type InPort k v = ReceivePort (Maybe (k,v))

type StatDict k v = Static (SerializableDict (Maybe (k,v)))

data WorkFlow k v k' v' = forall a b . (Serializable a, Serializable b) => WorkFlow (WorkFlow k v a b) (Worker a b k' v') | SingleWorker (Worker k v k' v') 

sdictString :: SerializableDict String
sdictString = SerializableDict

sdictUnit :: SerializableDict ()
sdictUnit = SerializableDict

makeMapF :: [a] -> (ByteString -> a)
makeMapF xs = (\bs -> xs !! ((fromIntegral $ integerDigest $ sha1 bs) `mod` (fromIntegral $ length xs))) 

sendAll :: Serializable a => [SendPort a] -> a -> Process ()
sendAll ps v = mapM_ (\p -> sendChan p v) ps

sendList :: (Binary a, Serializable a, Serializable b) => (ByteString -> OutPort a b) -> [(a,b)] -> Process ()
sendList f as = mapM_ (\(k,v) -> sendChan (f $ encode k) $ Just (k,v)) as

makeSource :: (Serializable k, Serializable v, Binary k) => (Handle -> IO [(k,v)]) -> ([OutPort k v] -> (ReceivePort String ->  Process ()))
makeSource reader = \sps -> (\rp -> do
  f <- receiveChan rp
  h <- liftIO $ openFile f ReadMode
  let findP = makeMapF sps 
  msg <- receiveChan rp
  case msg of
    "start" -> 
      let loop = do
          pairList <- liftIO $ reader h
          case pairList of
            [] -> sendAll sps (Nothing :: Maybe (k,v))
            otherwise -> sendList findP pairList >> loop
      in loop
    _ -> return () )

makeSink :: (Serializable k, Serializable v) => ((k,v) -> Handle -> IO ()) -> (FilePath -> InPort k v -> Process ())
makeSink writer = \f -> (\rp -> do
  h <- liftIO $ openFile f WriteMode
  let loop = do
      pair <- receiveChan rp
      case pair of
        Nothing -> do
          liftIO $ hClose h
          return ()
        Just (k,v) -> do
          liftIO $ writer (k,v) h
          loop
  loop )

makeMapper :: (Binary k', Serializable k, Serializable v, Serializable k', Serializable v') => (k -> v -> (k',v')) -> ([OutPort k' v'] -> InPort k v -> Process ())
makeMapper f = \sps -> (\rp -> do
  let next = makeMapF sps
  let loop 0 = sendAll sps Nothing
      loop n = do
        pair <- receiveChan rp
        case pair of
          Nothing -> loop $ n - 1
          Just (key,val) -> do
            let (key',val') = f key val
            sendChan (next (encode key')) (Just (key', val'))
            loop n
  loop $ length sps )

collector :: (Ord k, Serializable k, Serializable v) => InPort k v -> Int -> Process (Map k [v])
collector rp n_nodes = loop rp n_nodes empty where
  loop rp 0 m = return m
  loop rp n m = do
    pair <- receiveChan rp
    case pair of
      Nothing -> loop rp (n - 1) m
      Just (key,val) -> loop rp n $ insertWith (++) key [val] m

makeReducer :: (Ord k, Binary k', Serializable k, Serializable v, Serializable k', Serializable v') => (k -> [v] -> [(k',v')]) -> ([OutPort k' v'] -> InPort k v -> Process ())
makeReducer f = \sps -> (\rp -> do
  let findNext = makeMapF sps
  keyMap <- collector rp $ length sps
  let loop _ _ []                = sendAll sps Nothing
      loop f next ((k,vs) : kvs) = do
        let res = f k vs
        sendList next res
        loop f next kvs
  loop f findNext $ toList keyMap )

getPart :: FilePath -> Process ()
getPart f = do
  content <- expect
  liftIO $ hPutStrLn stderr $ "getting part " ++ f
  h <- liftIO $ openFile f WriteMode
  liftIO $ hPutStr h content
  liftIO $ hClose h

remotable ['getPart, 'sdictString]

spawnAll :: (Serializable k, Serializable v, Serializable k', Serializable v') => Worker k v k' v' -> [NodeId] -> [OutPort k' v'] -> Process [OutPort k v]
spawnAll _ [] _ = return []
spawnAll _ _ [] = return []
spawnAll w (n:ns) ps = do
  pid <- spawnChannel (sdict w) n $ (workClosure w) ps
  pids <- spawnAll w ns ps
  return (pid:pids)

iterMR :: (Serializable k, Serializable v, Serializable k', Serializable v') => [NodeId] -> [OutPort k' v'] -> WorkFlow k v k' v' -> Process [OutPort k v]
iterMR [] _ _ = return []
iterMR _ [] _ = return []
iterMR ns ps (SingleWorker w) = spawnAll w ns ps
iterMR ns ps (WorkFlow wf w) = do
  new_ps <- spawnAll w ns ps
  iterMR ns new_ps wf

sendPart :: (NodeId, FilePath) -> Process ()
sendPart (n,f) = do
  h <- liftIO $ openFile f ReadMode
  liftIO $ hPutStrLn stdout $ "sending part " ++ f
  pid <- spawn n ($(mkClosure 'getPart) f)
  contents <- liftIO $ hGetContents h
  send pid contents

splitFile :: FilePath -> [NodeId] -> Process [FilePath]
splitFile f nodes = do
  h <- liftIO $ openFile f ReadMode
  size <- liftIO $ hFileSize h
  liftIO $ hClose h
  let partSize = div size $ fromIntegral (length nodes)
  let cmd = "split -b " ++ (show partSize) ++ " " ++ f
  ret <- liftIO $ system cmd
  if ret /= ExitSuccess 
    then do
      fail $ cmd ++ " failed with error code " ++ (show ret)
    else do
      return ()
  let nodeParts = zip nodes $ getPartNames "x"
  liftIO $ hPutStrLn stdout $ show nodeParts
  mapM_ sendPart nodeParts
  return $ take (length nodes) $ getPartNames "x"

getPartNames :: FilePath -> [FilePath]
getPartNames f = let alpha = ['a'..'z'] in
  map ((++) f) $ concat $ map (\c -> map (\a -> [c]++[a]) alpha) alpha

pairToNode :: (String, String) -> NodeId
pairToNode (h,p) = makeNodeId $ h ++ ":" ++ p

runMapReduce :: (Serializable k1, Serializable v1, Serializable k2, Serializable v2) => FilePath -> RemoteTable -> MapReduceJob k1 v1 k2 v2 -> IO ()
runMapReduce f rtable mr = do
  (n_workers, mhost, mport, node_pairs) <- getVals
  backend    <- initializeBackend mhost mport rtable
  local <- newLocalNode backend
  let nodes = map pairToNode node_pairs
  hPutStrLn stdout "running map reduce"
  runProcess local $ do
    liftIO $ hPutStrLn stdout "splitting file"
    fileparts <- splitFile (file mr) nodes
    liftIO $ hPutStrLn stdout "starting sinks"
    let resnames = zip nodes $ getPartNames "result_"
    last_pids <- mapM (\(n,f) -> spawnChannel (endDict mr) n $ (writer mr) f) resnames
    liftIO $ hPutStrLn stdout "spawning workers"
    first_pids <- iterMR nodes last_pids $ workflow mr
    liftIO $ hPutStrLn stdout "starting sources"
    start_pids <- mapM (\(n) -> spawnChannel $(mkStatic 'sdictString) n $ (reader mr) first_pids) nodes
    liftIO $ hPutStrLn stdout "sending file names to sources"
    mapM_ (\(p,f) -> sendChan p f) $ zip start_pids fileparts
    liftIO $ hPutStrLn stdout "sending file names to sources"
    sendAll start_pids "start"

  where
    getVals :: IO (Int, String, String, [(String,String)])
    getVals = do
      cp_m <- Conf.readfile Conf.emptyCP f
      let cp        = forceEither cp_m
          n_workers = forceEither $ Conf.get cp "DEFAULT" "n_workers"
          mhost     = forceEither $ Conf.get cp "Master" "host"
          mport     = forceEither $ Conf.get cp "Master" "port"
      node_pairs <- go n_workers cp
      return (n_workers, mhost, mport, node_pairs)
      
    go :: Int -> Conf.ConfigParser -> IO [(String,String)]
    go 0 _ = return []
    go n conf = do
      let section = "Node " ++ (show n) 
          host    = forceEither $ Conf.get conf section "host"
          port    = forceEither $ Conf.get conf section "port"
      rest <- go (n-1) conf
      return ( (host,port) : rest )

runWorker :: String -> String -> RemoteTable -> IO ()
runWorker host port rtable = do
  backend <- initializeBackend host port rtable
  startSlave backend
