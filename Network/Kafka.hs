{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE NamedFieldPuns #-}

module Network.Kafka where

import Control.Concurrent.Chan
import Control.Concurrent (forkIO)
import Control.Exception (bracket)
import Control.Monad (liftM, forever, replicateM)
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import Data.Serialize.Get
import Data.Int
import Data.List (find, groupBy, sortBy)
import Data.Maybe (mapMaybe)
import Network
import System.IO

import Network.Kafka.Protocol

{-

Warning: none of this should actually be used in any way. This is basically a
scratch pad/dumping ground to try stuff out to make sure it worked against a
running Kafka instance. There are some usage examples in here, but they
certainly aren't easy to read. I'll work on extracting the valuable examples
into something useful.

-}

withConnection :: (Handle -> IO c) -> IO c
withConnection = withConnection' ("localhost", 9092)

withConnection' :: (HostName, PortNumber) -> (Handle -> IO c) -> IO c
withConnection' (host, port) = bracket (connectTo host $ PortNumber port) hClose

produceLots :: ByteString -> [ByteString] -> Handle -> IO [Either String Response]
produceLots t ms h = mapM go ms
  where
    go m = do
      let r = client $ produceRequest 0 t Nothing m
      -- threadDelay 200000
      req r h

produceLots' :: ByteString -> [ByteString] -> Handle -> IO ()
produceLots' t ms h = mapM_ go ms
  where
    go m = do
      let r = client $ produceRequest 0 t Nothing m
      -- threadDelay 200000
      req r h

producer :: (HostName, PortNumber) -> ByteString -> [ByteString] -> IO (Either String Response)
producer seed topic ms = do
  resp <- withConnection' seed $ req $ client $ metadataRequest [topic]
  leader <- case resp of
    Left s -> error s
    Right (Response (_, (MetadataResponse mr))) -> case leaderFor topic mr of
      Nothing -> error "uh, there should probably be a leader..."
      Just leader -> return leader
    _ -> error "omg!"
  -- let r = client . produceRequest 0 topic Nothing
      -- go m = threadDelay 200000 >> req . (r m)
  withConnection' leader $ req $ client $ produceRequests 0 topic ms

producer' :: (HostName, PortNumber) -> ByteString -> [ByteString] -> IO [Either String Response]
producer' seed topic ms = do
  resp <- withConnection' seed $ req $ client $ metadataRequest [topic]
  leader <- case resp of
    Left s -> error s
    Right (Response (_, (MetadataResponse mr))) -> case leaderFor topic mr of
      Nothing -> error "uh, there should probably be a leader..."
      Just leader -> return leader
    _ -> error "omg!"
  -- let r = client . produceRequest 0 topic Nothing
      -- go m = threadDelay 200000 >> req . (r m)
  withConnection' leader $ produceLots topic ms

producer'' :: (HostName, PortNumber) -> ByteString -> [ByteString] -> IO ()
producer'' seed topic ms = do
  resp <- withConnection' seed $ req $ client $ metadataRequest [topic]
  leader <- case resp of
    Left s -> error s
    Right (Response (_, (MetadataResponse mr))) -> case leaderFor topic mr of
      Nothing -> error "uh, there should probably be a leader..."
      Just leader -> return leader
    _ -> error "omg!"
  -- let r = client . produceRequest 0 topic Nothing
      -- go m = threadDelay 200000 >> req . (r m)
  withConnection' leader $ produceLots' topic ms

produceStuff :: (HostName, PortNumber) -> ByteString -> [ByteString] -> IO [Either String Response]
produceStuff seed topic ms = do
  resp <- withConnection' seed $ req $ client $ metadataRequest [topic]
  -- pattern matches here aren't exhaustive
  -- do we need to return an error if it's not a Metadata response?
  leaders <- case resp of
    Left s -> error s
    Right (Response (_, (MetadataResponse mr))) -> return $ leadersFor topic mr
  chans <- replicateM (length leaders) newChan
  outChan <- newChan
  -- what's this doing?
  -- let actions = map (\(p, leader) -> (p, withConnection' leader)) leaders
  --     actions' = zip chans actions
  mapM_ (\ (chan, (p, (host, port))) -> forkIO $ do
    h <- connectTo host $ PortNumber port
    forever (readChan chan >>= ((flip req) h . client . produceRequests p topic) >>= writeChan outChan)) (zip chans leaders)

  -- this could use splitting out w/ explicit type. What is it comparing?
  let mss = map (\xs -> map snd xs) $ groupBy (\ (x,_) (y,_) -> x == y) $ sortBy (\ (x,_) (y,_) -> x `compare` y) $ zip (cycle [1..5]) ms

  mapM_ (\ (chan, ms') -> writeChan chan ms') (zip (cycle chans) mss)
  replicateM (length mss) (readChan outChan)

-- |As long as the supplied "Maybe" expression returns "Just _", the loop
-- body will be called and passed the value contained in the 'Just'.  Results
-- are discarded.
whileJust_ :: (Monad m) => m (Maybe a) -> (a -> m b) -> m ()
whileJust_ p f = go
    where go = do
            x <- p
            case x of
                Nothing -> return ()
                Just w  -> do
                  _ <- f w
                  go

chanReq :: Chan (Maybe a) -> Chan b -> (a -> IO b) -> IO ()
chanReq cIn cOut f = do whileJust_ (readChan cIn) $ \msg -> f msg >>= writeChan cOut

transformChan :: (a -> IO b) -> Chan (Maybe a) -> IO (Chan b)
transformChan f cIn = do
  cOut <- newChan
  _ <- forkIO $ whileJust_ (readChan cIn) $ \msg -> f msg >>= writeChan cOut
  return cOut

-- leadersFor :: ByteString -> MetadataResponse -> [(Partition, (String, PortNumber))]
leadersFor :: Num a => ByteString -> MetadataResponse -> [(a, (String, PortNumber))]
leadersFor topicName (MetadataResp (bs, ts)) =
  let bs' = map (\(Broker (NodeId x, (Host (KString h)), (Port p))) -> (x, (B.unpack h, fromIntegral p))) bs
      isTopic (TopicMetadata (e, TName (KString tname),_)) =
        tname == topicName && e == NoError
  in case find isTopic ts of
    (Just (TopicMetadata (_, _, ps))) ->
      mapMaybe (\(PartitionMetadata (pErr, Partition pid, Leader (Just leaderId), _, _)) ->
        (if pErr == NoError then lookup leaderId bs' else Nothing) >>= \x -> return (fromIntegral pid, x)) ps
    Nothing -> []

leaderFor :: ByteString -> MetadataResponse -> Maybe (String, PortNumber)
leaderFor topicName (MetadataResp (bs, ts)) = do
  let bs' = map (\(Broker (NodeId x, (Host (KString h)), (Port p))) -> (x, (B.unpack h, fromIntegral p))) bs
      isTopic (TopicMetadata (_, TName (KString tname),_)) = tname == topicName
      isPartition (PartitionMetadata (_, Partition x, _, _, _)) = x == 0
  (TopicMetadata (_, _, ps)) <- find isTopic ts
  -- null op
  -- if tErr /= NoError then Nothing else Just tErr
  (PartitionMetadata (_, _, Leader (Just leaderId), _, _)) <- find isPartition ps
  -- null op?
  -- if pErr /= NoError then Nothing else Just tErr
  lookup leaderId bs'

-- Response (CorrelationId 0,MetadataResponse (MetadataResp (
-- [Broker (NodeId 1,Host (KString "192.168.33.1"),Port 9093),Broker (NodeId 0,Host (KString "192.168.33.1"),Port 9092),Broker (NodeId 2,Host (KString "192.168.33.1"),Port 9094)],
-- [TopicMetadata (
--   ErrorCode 0,
--   TName (KString "replicated"),
--   [PartitionMetadata (
--     ErrorCode 0,
--     Partition 0,
--     Leader (Just 1),
--     Replicas [1,0,2],
--     Isr [1,0])])])))

-- Response (CorrelationId 0,ProduceResponse (ProduceResp [(TName (KString "replicated"),[(Partition 0,ErrorCode 6,Offset (-1))])]))

req :: Request -> Handle -> IO (Either String Response)
req r h = do
  let bytes = requestBytes r
  B.hPut h bytes
  hFlush h
  let reader = B.hGet h
  rawLength <- reader 4
  let (Right dataLength) = runGet (liftM fromIntegral getWord32be) rawLength
  resp <- reader dataLength
  return $ runGet (getResponse dataLength) resp

reqbs :: Request -> Handle -> IO ByteString
reqbs r h = do
  let bytes = requestBytes r
  B.hPut h bytes
  hFlush h
  let reader = B.hGet h
  rawLength <- reader 4
  let (Right dataLength) = runGet (liftM fromIntegral getWord32be) rawLength
  resp <- reader dataLength
  return resp

{-
  The rest of the file used to live in the Protocol module. Most of it is
  scratchpad stuff to make all the newtypes easier to work with in GHCi. In
  the interest of making the Protocol module easier to understand, I just
  shoved them all in here for now.
-}

client :: RequestMessage -> Request
client = request "kafkah"

request :: KafkaString -> RequestMessage -> Request
request clientId m = Request (CorrelationId 0, ClientId clientId, m)

metadata :: MetadataResponse -> [(TopicName, [(Partition, Broker)])]
metadata (MetadataResp (bs, ts)) = omg ts
  where
    broker nodeId = lookup nodeId $ map (\ b@(Broker (NodeId x, _, _)) -> (x, b)) bs
    withoutErrors = mapMaybe $ \t@(TopicMetadata (_, n, ps)) -> if hasError t then Just (n, ps) else Nothing
    pbs (PartitionMetadata (_, p, Leader l, _, _)) = l >>= broker >>= return . (p,)
    omg = map (\ (name, ps) -> (name, mapMaybe pbs ps)) . withoutErrors

message :: Message -> Maybe ByteString
message (Message (_, _, _, _, Value (MKB (Just (KBytes bs))))) = Just bs
message (Message (_, _, _, _, Value (MKB Nothing))) = Nothing

class HasError a where
  hasError :: a -> Bool

instance HasError TopicMetadata where hasError (TopicMetadata (e, _, _)) = e /= NoError
instance HasError PartitionMetadata where hasError (PartitionMetadata (e, _, _, _, _)) = e /= NoError

ocr :: ConsumerGroup -> Offset -> RequestMessage
ocr g offset = OffsetCommitRequest $ OffsetCommitReq (g, [("test", [(0, offset, -1, "SO META!")])])

ofr :: ConsumerGroup -> RequestMessage
ofr g = OffsetFetchRequest $ OffsetFetchReq (g, [("test", [0])])

cmr :: ConsumerGroup -> RequestMessage
cmr g = ConsumerMetadataRequest $ ConsumerMetadataReq g

-- jgr g = JoinGroupRequest $ JoinGroupReq (g, Timeout 10000, ["join-group-test"], ConsumerId "", PartitionAssignmentStrategy "strategy1")

offsetRequest :: RequestMessage
offsetRequest = OffsetRequest $ OffsetReq (-1, [("test", [(0, -1, 100)])])
offsetRequest' :: RequestMessage
offsetRequest' = OffsetRequest $ OffsetReq (-1, [("test", [(0, -2, 100)])])
offsetRequest'' :: RequestMessage
offsetRequest'' = OffsetRequest $ OffsetReq (-1, [("test", [(0, maxBound, 100)])])

fetchRequest :: RequestMessage
fetchRequest = FetchRequest $ FetchReq (ReplicaId (-1), MaxWaitTime 15000, MinBytes 1, [(TName (KString "test"), [(Partition 0, Offset 104, MaxBytes 10000)]), (TName (KString "example"), [(Partition 0, Offset 4, MaxBytes 10000)])])

data OffsetParam = Earliest
                 | Latest
                 | OffsetN Offset
                 deriving (Show)

data FetchOpts = FetchOpts { maxWait :: Int32
                           , minBytes :: Int32
                           , maxBytes :: Int32}

defaultFetchOpts :: FetchOpts
defaultFetchOpts = FetchOpts { maxWait = 100 -- milliseconds
                             , minBytes = 1
                             , maxBytes = 1048576 -- 1MB
                             }

fr :: FetchOpts -> [(ByteString, Int32, Int64)] -> RequestMessage
fr (FetchOpts {maxWait, minBytes, maxBytes}) xs =
  FetchRequest $ FetchReq (ReplicaId (-1), MaxWaitTime maxWait, MinBytes minBytes, ts)
    where ts = map (\ (t, p, o) -> (TName (KString t), [(Partition p, Offset o, MaxBytes maxBytes)])) xs

-- OPTION_DEFAULTS = {
--   :compression_codec => nil,
--   :compressed_topics => nil,
--   :metadata_refresh_interval_ms => 600_000,
--   :partitioner => nil,
--   :max_send_retries => 3,
--   :retry_backoff_ms => 100,
--   :required_acks => 0,
--   :ack_timeout_ms => 1500,
-- }


-- produceRequest :: Key -> Value -> RequestMessage
-- produceRequest k v = ProduceRequest $ ProduceReq (1, 10000, [("test", [(0, [MessageSetMember (0, (Message (1, 0, 0, k, v)))])])])

produceRequest :: Int32 -> ByteString -> Maybe KafkaBytes -> ByteString -> RequestMessage
produceRequest p topic k m = ProduceRequest $ ProduceReq (1, 10000, [((TName (KString topic)), [(Partition p, MessageSet [MessageSetMember (0, (Message (1, 0, 0, (Key (MKB k)), (Value (MKB (Just (KBytes m)))))))])])])
-- RequiredAcks 1,Timeout 10000,[(TName (KString "test"),[(Partition 0,
--   MessageSet [MessageSetMember (Offset 0,Message (Crc 1,MagicByte 0,Attributes 0,Key (MKB Nothing),Value (MKB (Just (KBytes "hi")))))])])]

produceRequests :: Int32 -> ByteString -> [ByteString] -> RequestMessage
produceRequests p topic ms = ProduceRequest $ ProduceReq (1, 10000, [((TName (KString topic)), [(Partition p, MessageSet ms')])])
  where ms' = map (\m -> MessageSetMember (0, (Message (1, 0, 0, (Key (MKB Nothing)), (Value (MKB (Just (KBytes m)))))))) ms

metadataRequest :: [ByteString] -> RequestMessage
metadataRequest ts = MetadataRequest $ MetadataReq $ map (TName . KString) ts
