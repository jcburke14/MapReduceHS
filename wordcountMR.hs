{-# LANGUAGE TemplateHaskell #-}

import MapReduce hiding (__remoteTable)
import qualified MapReduce as MR (__remoteTable)
import Data.Typeable
import Control.Distributed.Process
import Control.Distributed.Process.Node
import Control.Distributed.Process.Backend.SimpleLocalnet
import Control.Distributed.Process.Serializable
import Control.Distributed.Process.Closure
import System.Environment
import System.IO

wordCountReader ::([OutPort String String] -> (ReceivePort String -> Process ()))
wordCountReader = makeSource (\h -> do
  contents <- hGetContents h
  hPutStrLn stderr "Read from file"
  return $ map (\w -> ("from file", w)) $ words contents )

wordCountWriter :: (FilePath -> InPort String Int -> Process ())
wordCountWriter = makeSink (\pair h -> hPutStr h $ show pair)

countMap :: ([(OutPort String Int)] -> InPort String String -> Process ()) 
countMap = makeMapper (\_ word -> (word, 1))

countReduce :: ([(OutPort String Int)] -> InPort String Int -> Process ()) 
countReduce = makeReducer (\word countList -> [(word, (length countList))])

sdictStrStr :: SerializableDict (Maybe (String, String))
sdictStrStr = SerializableDict

sdictStrInt :: SerializableDict (Maybe (String, Int))
sdictStrInt = SerializableDict

remotable ['wordCountReader, 'wordCountWriter, 'countMap, 'countReduce, 'sdictStrStr, 'sdictStrInt]

countMapper = Worker ($(mkClosure 'countMap)) ($(mkStatic 'sdictStrStr))

countReducer = Worker ($(mkClosure 'countReduce)) ($(mkStatic 'sdictStrInt))

wordCountMR :: FilePath -> MapReduceJob String String String Int
wordCountMR = \f -> MapReduce f ($(mkClosure 'wordCountReader)) ($(mkClosure 'wordCountWriter)) (WorkFlow (SingleWorker countMapper) countReducer) ($(mkStatic 'sdictStrInt))

rtable :: RemoteTable
rtable = MR.__remoteTable $ initRemoteTable

--get args, handle slaves here
main = do
  args <- getArgs
  case args of
    "master" : confFile : inputFile : [] -> runMapReduce confFile rtable $ wordCountMR inputFile
    "slave" : host : port : [] -> runWorker host port rtable

