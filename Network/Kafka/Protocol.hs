{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TupleSections #-}

module Network.Kafka.Protocol where

-- module Network.Kafka.Protocol (
--   requestBytes,
--   RequestMessage,
--   metadataRequest,
--   Serializable(..),
--   Deserializable(..)) where

import Data.Int
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import Data.Serialize.Put
import Data.Serialize.Get
import Prelude hiding ((.), id)
import Control.Category (Category(..))
import Control.Monad (replicateM, liftM, liftM2, liftM3, liftM4, liftM5)
import Control.Applicative (Alternative(..))
import GHC.Exts(IsString(..), IsList(..))
import Data.Digest.CRC32
import Data.Maybe (mapMaybe)

-- import Debug.Trace

-- traceShowM' :: (Show a, Monad m) => String -> a -> m ()
-- traceShowM' s x = traceM (s ++ " " ++ show x)
-- traceShow' :: Show a => String -> a -> a
-- traceShow' s x = trace (s ++ " " ++ show x) x

client :: RequestMessage -> Request
client = request "kafkah"

metadata :: MetadataResponse -> [(TopicName, [(Partition, Broker)])]
metadata (MetadataResp (bs, ts)) = omg ts
  where
    broker nodeId = lookup nodeId $ map (\ b@(Broker (NodeId x, _, _)) -> (x, b)) bs
    withoutErrors = mapMaybe $ \t@(TopicMetadata (_, n, ps)) -> if hasError t then Just (n, ps) else Nothing
    pbs (PartitionMetadata (_, p, Leader l, _, _)) = l >>= broker >>= return . (p,)
    omg = map (\ (name, ps) -> (name, mapMaybe pbs ps)) . withoutErrors

class HasError a where
  hasError :: a -> Bool

instance HasError TopicMetadata where hasError (TopicMetadata (e, _, _)) = e /= NoError
instance HasError PartitionMetadata where hasError (PartitionMetadata (e, _, _, _, _)) = e /= NoError

-- TopicMetadata ( NoError
--               , TName (KString "partitioned")
--               , [ PartitionMetadata (NoError,Partition 2,Leader (Just 2),Replicas [2],Isr [2])
--                 , PartitionMetadata (NoError,Partition 1,Leader (Just 1),Replicas [1],Isr [1])
--                 , PartitionMetadata (NoError,Partition 0,Leader (Just 0),Replicas [0],Isr [0])])

-- MetadataResp ( [ Broker (NodeId 2,Host (KString "192.168.33.1"),Port 9094)
--                , Broker (NodeId 1,Host (KString "192.168.33.1"),Port 9093)
--                , Broker (NodeId 0,Host (KString "192.168.33.1"),Port 9092)]
--              , [])

request :: KafkaString -> RequestMessage -> Request
request clientId m = Request (CorrelationId 0, ClientId clientId, m)

ocr :: Offset -> RequestMessage
ocr offset = OffsetCommitRequest $ OffsetCommitReq ("group", [("test", [(0, offset, -1, "SO META!")])])

ofr :: RequestMessage
ofr = OffsetFetchRequest $ OffsetFetchReq ("group", [("test", [0])])

offsetRequest :: RequestMessage
offsetRequest = OffsetRequest $ OffsetReq (-1, [("test", [(0, -1, 100)])])
offsetRequest' :: RequestMessage
offsetRequest' = OffsetRequest $ OffsetReq (-1, [("test", [(0, -2, 100)])])
offsetRequest'' :: RequestMessage
offsetRequest'' = OffsetRequest $ OffsetReq (-1, [("test", [(0, maxBound, 100)])])

fetchRequest :: RequestMessage
fetchRequest = FetchRequest $ FetchReq (ReplicaId (-1), MaxWaitTime 15000, MinBytes 1, [(TName (KString "test"), [(Partition 0, Offset 104, MaxBytes 10000)]), (TName (KString "example"), [(Partition 0, Offset 4, MaxBytes 10000)])])

--  Create a new consumer which reads the specified topic and partition from
--  the host.
-- 
--  @param [String] client_id  Used to identify this client should be unique.
--  @param [String] host
--  @param [Integer] port 
--  @param [String] topic Topic to read from
--  @param [Integer] partition Partitions are zero indexed.
--  @param [Integer,Symbol] offset 
--    Offset to start reading from. A negative offset can also be passed.
--    There are a couple special offsets which can be passed as symbols:
--      :earliest_offset       Start reading from the first offset the server has.
--      :latest_offset         Start reading from the latest offset the server has.
-- 
--  @param [Hash] options
--    Theses options can all be overridden in each individual fetch command.
-- 
--  @option options [:max_bytes] Maximum number of bytes to fetch
--    Default: 1048576 (1MB)
-- 
--  @option options [:max_wait_ms] 
--    How long to block until the server sends us data.
--    NOTE: This is only enforced if min_bytes is > 0.
--    Default: 100 (100ms)
-- 
--  @option options [:min_bytes] Smallest amount of data the server should send us.
--    Default: 1 (Send us data as soon as it is ready)
-- 
--  @api public

data OffsetParam = Earliest
                 | Latest
                 | OffsetN Offset

fr maxWait minBytes maxBytes topic p o = FetchRequest $ FetchReq (ReplicaId (-1), MaxWaitTime maxWait, MinBytes minBytes, ts)
  where ts = [(TName (KString topic), [(Partition p, Offset o, MaxBytes maxBytes)])]

fetchMany maxWait minBytes maxBytes topics p o = FetchRequest $ FetchReq (ReplicaId (-1), MaxWaitTime maxWait, MinBytes minBytes, ts)
  where ts = map (\topic -> (TName (KString topic), [(Partition p, Offset o, MaxBytes maxBytes)])) topics

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

produceRequest p topic k m = ProduceRequest $ ProduceReq (1, 10000, [((TName (KString topic)), [(Partition p, [MessageSetMember (0, (Message (1, 0, 0, (Key (MKB k)), (Value (MKB (Just (KBytes m)))))))])])])
-- RequiredAcks 1,Timeout 10000,[(TName (KString "test"),[(Partition 0,
--   MessageSet [MessageSetMember (Offset 0,Message (Crc 1,MagicByte 0,Attributes 0,Key (MKB Nothing),Value (MKB (Just (KBytes "hi")))))])])]

produceRequests p topic ms = ProduceRequest $ ProduceReq (1, 10000, [((TName (KString topic)), [(Partition p, MessageSet ms')])])
  where ms' = map (\m -> MessageSetMember (0, (Message (1, 0, 0, (Key (MKB Nothing)), (Value (MKB (Just (KBytes m)))))))) ms

metadataRequest :: [ByteString] -> RequestMessage
metadataRequest ts = MetadataRequest $ MetadataReq $ map (TName . KString) ts

class Serializable a where
  serialize :: a -> Put

class Deserializable a where
  deserialize :: Get a

newtype Response = Response (CorrelationId, ResponseMessage) deriving (Show, Eq)

getResponse :: Int -> Get Response
getResponse l = do
  correlationId <- deserialize
  rm <- getResponseMessage (l - 4)
  return $ Response (correlationId, rm)

data ResponseMessage = MetadataResponse MetadataResponse
                     | ProduceResponse ProduceResponse
                     | FetchResponse FetchResponse
                     | OffsetResponse OffsetResponse
                     | OffsetCommitResponse OffsetCommitResponse
                     | OffsetFetchResponse OffsetFetchResponse
                     deriving (Show, Eq)

getResponseMessage :: Int -> Get ResponseMessage
getResponseMessage l = liftM MetadataResponse     (isolate l deserialize)
                   <|> liftM OffsetResponse       (isolate l deserialize)
                   <|> liftM ProduceResponse      (isolate l deserialize)
                   <|> liftM OffsetCommitResponse (isolate l deserialize)
                   <|> liftM OffsetFetchResponse  (isolate l deserialize)
                   -- MUST try FetchResponse last!
                   --
                   -- As an optimization, Kafka might return a partial message
                   -- at the end of a MessageSet, so this will consume the rest
                   -- of the message at the end of the input.
                   --
                   -- Strictly speaking, this might not actually be necessary.
                   -- Parsing a MessageSet is isolated to the byte count that's
                   -- at the beginning of a MessageSet. I don't want to spend
                   -- the time right now to prove that will always be safe, but
                   -- I'd like to at some point.
                   <|> liftM FetchResponse        (isolate l deserialize)

newtype ApiKey = ApiKey Int16 deriving (Show, Eq, Deserializable, Serializable, Num) -- numeric ID for API (i.e. metadata req, produce req, etc.)
newtype ApiVersion = ApiVersion Int16 deriving (Show, Eq, Deserializable, Serializable, Num)
newtype CorrelationId = CorrelationId Int32 deriving (Show, Eq, Deserializable, Serializable, Num)
newtype ClientId = ClientId KafkaString deriving (Show, Eq, Deserializable, Serializable, IsString)

data RequestMessage = MetadataRequest MetadataRequest
                    | ProduceRequest ProduceRequest
                    | FetchRequest FetchRequest
                    | OffsetRequest OffsetRequest
                    | OffsetCommitRequest OffsetCommitRequest
                    | OffsetFetchRequest OffsetFetchRequest
                    -- | ConsumerMetadataRequest ConsumerMetadataRequest
                    deriving (Show, Eq)

newtype MetadataRequest = MetadataReq [TopicName] deriving (Show, Eq, Serializable, Deserializable)
newtype TopicName = TName KafkaString deriving (Show, Eq, Deserializable, Serializable, IsString)

newtype KafkaBytes = KBytes ByteString deriving (Show, Eq, IsString)
newtype KafkaString = KString ByteString deriving (Show, Eq, IsString)

newtype ProduceResponse = ProduceResp [(TopicName, [(Partition, KafkaError, Offset)])] deriving (Show, Eq, Deserializable, Serializable)

newtype OffsetResponse = OffsetResp [(TopicName, [PartitionOffsets])] deriving (Show, Eq, Deserializable)
newtype PartitionOffsets = PartitionOffsets (Partition, KafkaError, [Offset]) deriving (Show, Eq, Deserializable)

newtype FetchResponse = FetchResp [(TopicName, [(Partition, KafkaError, Offset, MessageSet)])] deriving (Show, Eq, Serializable, Deserializable)

-- newtype ErrorCode = ErrorCode Int16 deriving (Show, Eq, Serializable, Deserializable, Num)

newtype MetadataResponse = MetadataResp ([Broker], [TopicMetadata]) deriving (Show, Eq, Deserializable)
newtype Broker = Broker (NodeId, Host, Port) deriving (Show, Eq, Deserializable)
newtype NodeId = NodeId Int32 deriving (Show, Eq, Deserializable, Num)
newtype Host = Host KafkaString deriving (Show, Eq, Deserializable, IsString)
newtype Port = Port Int32 deriving (Show, Eq, Deserializable, Num)
newtype TopicMetadata = TopicMetadata (KafkaError, TopicName, [PartitionMetadata]) deriving (Show, Eq, Deserializable)
newtype PartitionMetadata = PartitionMetadata (KafkaError, Partition, Leader, Replicas, Isr) deriving (Show, Eq, Deserializable)
newtype Leader = Leader (Maybe Int32) deriving (Show, Eq)

newtype Replicas = Replicas [Int32] deriving (Show, Eq, Serializable, Deserializable)
newtype Isr = Isr [Int32] deriving (Show, Eq, Deserializable)

newtype OffsetCommitResponse = OffsetCommitResp [(TopicName, [(Partition, KafkaError)])] deriving (Show, Eq, Deserializable)
newtype OffsetFetchResponse = OffsetFetchResp [(TopicName, [(Partition, Offset, Metadata, KafkaError)])] deriving (Show, Eq, Deserializable)

newtype OffsetRequest = OffsetReq (ReplicaId, [(TopicName, [(Partition, Time, MaxNumberOfOffsets)])]) deriving (Show, Eq, Serializable)
newtype Time = Time Int64 deriving (Show, Eq, Serializable, Num, Bounded)
newtype MaxNumberOfOffsets = MaxNumberOfOffsets Int32 deriving (Show, Eq, Serializable, Num)

newtype FetchRequest = FetchReq (ReplicaId, MaxWaitTime, MinBytes, [(TopicName, [(Partition, Offset, MaxBytes)])]) deriving (Show, Eq, Deserializable, Serializable)

newtype ReplicaId = ReplicaId Int32 deriving (Show, Eq, Num, Serializable, Deserializable)
newtype MaxWaitTime = MaxWaitTime Int32 deriving (Show, Eq, Num, Serializable, Deserializable)
newtype MinBytes = MinBytes Int32 deriving (Show, Eq, Num, Serializable, Deserializable)
newtype MaxBytes = MaxBytes Int32 deriving (Show, Eq, Num, Serializable, Deserializable)

newtype ProduceRequest = ProduceReq (RequiredAcks, Timeout, [(TopicName, [(Partition, MessageSet)])]) deriving (Show, Eq, Serializable)

newtype RequiredAcks = RequiredAcks Int16 deriving (Show, Eq, Serializable, Deserializable, Num)
newtype Timeout = Timeout Int32 deriving (Show, Eq, Serializable, Deserializable, Num)
newtype Partition = Partition Int32 deriving (Show, Eq, Serializable, Deserializable, Num)

newtype MessageSet = MessageSet [MessageSetMember] deriving (Show, Eq)
newtype MessageSetMember = MessageSetMember (Offset, Message) deriving (Show, Eq)
newtype Offset = Offset Int64 deriving (Show, Eq, Serializable, Deserializable, Num)

newtype Message = Message (Crc, MagicByte, Attributes, Key, Value) deriving (Show, Eq, Deserializable)
newtype Crc = Crc Int32 deriving (Show, Eq, Serializable, Deserializable, Num)
newtype MagicByte = MagicByte Int8 deriving (Show, Eq, Serializable, Deserializable, Num)
newtype Attributes = Attributes Int8 deriving (Show, Eq, Serializable, Deserializable, Num)
newtype Key = Key MaybeKafkaBytes deriving (Show, Eq, Serializable, Deserializable)
newtype Value = Value MaybeKafkaBytes deriving (Show, Eq, Serializable, Deserializable)

newtype MaybeKafkaBytes = MKB (Maybe KafkaBytes) deriving (Show, Eq)

-- newtype ConsumerMetadataRequest = ConsumerMetadataReq ConsumerGroup deriving (Show, Eq, Serializable)
newtype OffsetCommitRequest = OffsetCommitReq (ConsumerGroup, [(TopicName, [(Partition, Offset, Time, Metadata)])]) deriving (Show, Eq, Serializable)
newtype OffsetFetchRequest = OffsetFetchReq (ConsumerGroup, [(TopicName, [Partition])]) deriving (Show, Eq, Serializable)
newtype ConsumerGroup = ConsumerGroup KafkaString deriving (Show, Eq, Serializable, Deserializable, IsString)
newtype Metadata = Metadata KafkaString deriving (Show, Eq, Serializable, Deserializable, IsString)

errorKafka :: KafkaError -> Int16
errorKafka NoError                             = 0
errorKafka Unknown                             = (-1)
errorKafka OffsetOutOfRange                    = 1
errorKafka InvalidMessage                      = 2
errorKafka UnknownTopicOrPartition             = 3
errorKafka InvalidMessageSize                  = 4
errorKafka LeaderNotAvailable                  = 5
errorKafka NotLeaderForPartition               = 6
errorKafka RequestTimedOut                     = 7
errorKafka BrokerNotAvailable                  = 8
errorKafka ReplicaNotAvailable                 = 9
errorKafka MessageSizeTooLarge                 = 10
errorKafka StaleControllerEpochCode            = 11
errorKafka OffsetMetadataTooLargeCode          = 12
errorKafka OffsetsLoadInProgressCode           = 14
errorKafka ConsumerCoordinatorNotAvailableCode = 15
errorKafka NotCoordinatorForConsumerCode       = 16

data KafkaError = NoError -- 0 No error--it worked!
                | Unknown -- -1 An unexpected server error
                | OffsetOutOfRange -- 1 The requested offset is outside the range of offsets maintained by the server for the given topic/partition.
                | InvalidMessage -- 2 This indicates that a message contents does not match its CRC
                | UnknownTopicOrPartition -- 3 This request is for a topic or partition that does not exist on this broker.
                | InvalidMessageSize -- 4 The message has a negative size
                | LeaderNotAvailable -- 5 This error is thrown if we are in the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes.
                | NotLeaderForPartition -- 6 This error is thrown if the client attempts to send messages to a replica that is not the leader for some partition. It indicates that the clients metadata is out of date.
                | RequestTimedOut -- 7 This error is thrown if the request exceeds the user-specified time limit in the request.
                | BrokerNotAvailable -- 8 This is not a client facing error and is used mostly by tools when a broker is not alive.
                | ReplicaNotAvailable -- 9 If replica is expected on a broker, but is not.
                | MessageSizeTooLarge -- 10 The server has a configurable maximum message size to avoid unbounded memory allocation. This error is thrown if the client attempt to produce a message larger than this maximum.
                | StaleControllerEpochCode -- 11 Internal error code for broker-to-broker communication.
                | OffsetMetadataTooLargeCode -- 12 If you specify a string larger than configured maximum for offset metadata
                | OffsetsLoadInProgressCode -- 14 The broker returns this error code for an offset fetch request if it is still loading offsets (after a leader change for that offsets topic partition).
                | ConsumerCoordinatorNotAvailableCode -- 15 The broker returns this error code for consumer metadata requests or offset commit requests if the offsets topic has not yet been created.
                | NotCoordinatorForConsumerCode -- 16 The broker returns this error code if it receives an offset fetch or commit request for a consumer group that it is not a coordinator for.
                deriving (Eq, Show)

instance Serializable KafkaError where
  serialize = serialize . errorKafka

instance Deserializable KafkaError where
  deserialize = do
    x <- deserialize :: Get Int16
    case x of
      0    -> return NoError
      (-1) -> return Unknown
      1    -> return OffsetOutOfRange
      2    -> return InvalidMessage
      3    -> return UnknownTopicOrPartition
      4    -> return InvalidMessageSize
      5    -> return LeaderNotAvailable
      6    -> return NotLeaderForPartition
      7    -> return RequestTimedOut
      8    -> return BrokerNotAvailable
      9    -> return ReplicaNotAvailable
      10   -> return MessageSizeTooLarge
      11   -> return StaleControllerEpochCode
      12   -> return OffsetMetadataTooLargeCode
      14   -> return OffsetsLoadInProgressCode
      15   -> return ConsumerCoordinatorNotAvailableCode
      16   -> return NotCoordinatorForConsumerCode
      _    -> fail $ "invalid error code: " ++ show x

-- omginstances...

newtype Request = Request (CorrelationId, ClientId, RequestMessage) deriving (Show, Eq)

instance Serializable Request where
  serialize (Request (correlationId, clientId, r)) = do
    serialize (apiKey r)
    serialize (apiVersion r)
    serialize correlationId
    serialize clientId
    serialize r

requestBytes :: Request -> ByteString
requestBytes x = runPut $ do
  putWord32be . fromIntegral $ B.length mr
  putByteString mr
    where mr = runPut $ serialize x

apiVersion :: RequestMessage -> ApiVersion
apiVersion _ = ApiVersion 0 -- everything is at version 0 right now

apiKey :: RequestMessage -> ApiKey
apiKey (ProduceRequest{}) = ApiKey 0
apiKey (FetchRequest{}) = ApiKey 1
apiKey (OffsetRequest{}) = ApiKey 2
apiKey (MetadataRequest{}) = ApiKey 3
apiKey (OffsetCommitRequest{}) = ApiKey 8
apiKey (OffsetFetchRequest{}) = ApiKey 9
-- apiKey (ConsumerMetadataRequest{}) = ApiKey 10

instance Serializable RequestMessage where
  serialize (ProduceRequest r) = serialize r
  serialize (FetchRequest r) = serialize r
  serialize (OffsetRequest r) = serialize r
  serialize (MetadataRequest r) = serialize r
  serialize (OffsetCommitRequest r) = serialize r
  serialize (OffsetFetchRequest r) = serialize r

instance Serializable Int64 where serialize = putWord64be . fromIntegral
instance Serializable Int32 where serialize = putWord32be . fromIntegral
instance Serializable Int16 where serialize = putWord16be . fromIntegral
instance Serializable Int8  where serialize = putWord8    . fromIntegral

instance Serializable MaybeKafkaBytes where
  serialize (MKB (Just kbs)) = serialize kbs
  serialize (MKB Nothing) = serialize (-1 :: Int32)

instance Serializable KafkaString where
  serialize (KString bs) = do
    let l = fromIntegral (B.length bs) :: Int16
    serialize l
    putByteString bs

instance Serializable MessageSet where
  serialize (MessageSet ms) = do
    let bytes = runPut $ mapM_ serialize ms
        l = fromIntegral (B.length bytes) :: Int32
    serialize l
    putByteString bytes

instance Serializable KafkaBytes where
  serialize (KBytes bs) = do
    let l = fromIntegral (B.length bs) :: Int32
    serialize l
    putByteString bs

instance Serializable MessageSetMember where
  serialize (MessageSetMember (offset, msg)) = do
    serialize offset
    serialize msize
    serialize msg
      where msize = fromIntegral $ B.length $ runPut $ serialize msg :: Int32

instance Serializable Message where
  serialize (Message (_, magic, attrs, k, v)) = do
    let m = runPut $ serialize magic >> serialize attrs >> serialize k >> serialize v
    putWord32be (crc32 m)
    putByteString m

instance (Serializable a) => Serializable [a] where
  serialize xs = do
    let l = fromIntegral (length xs) :: Int32
    serialize l
    mapM_ serialize xs

instance (Serializable a, Serializable b) => Serializable ((,) a b) where
  serialize (x, y) = serialize x >> serialize y
instance (Serializable a, Serializable b, Serializable c) => Serializable ((,,) a b c) where
  serialize (x, y, z) = serialize x >> serialize y >> serialize z
instance (Serializable a, Serializable b, Serializable c, Serializable d) => Serializable ((,,,) a b c d) where
  serialize (w, x, y, z) = serialize w >> serialize x >> serialize y >> serialize z
instance (Serializable a, Serializable b, Serializable c, Serializable d, Serializable e) => Serializable ((,,,,) a b c d e) where
  serialize (v, w, x, y, z) = serialize v >> serialize w >> serialize x >> serialize y >> serialize z

instance Deserializable MessageSet where
  deserialize = do
    l <- deserialize :: Get Int32
    ms <- isolate (fromIntegral l) getMembers
    return $ MessageSet ms

getMembers :: Get [MessageSetMember]
getMembers = do
  empty <- isEmpty
  if empty
  then return []
  else liftM2 (:) deserialize getMembers <|> (remaining >>= getBytes >> return [])
  -- else liftM2 (:) deserialize getMembers <|> (remaining >>= getBytes >>= traceShowM' "OMFG!" >> return [])

instance Deserializable MessageSetMember where
  deserialize = do
    o <- deserialize
    l <- deserialize :: Get Int32
    m <- isolate (fromIntegral l) deserialize
    return $ MessageSetMember (o, m)

instance Deserializable Leader where
  deserialize = do
    x <- deserialize :: Get Int32
    let l = if x == -1 then Leader Nothing else Leader $ Just x
    return l

instance Deserializable KafkaBytes where
  deserialize = do
    l <- deserialize :: Get Int32
    bs <- getByteString $ fromIntegral l
    return $ KBytes bs

instance Deserializable KafkaString where
  deserialize = do
    l <- deserialize :: Get Int16
    bs <- getByteString $ fromIntegral l
    return $ KString bs

instance Deserializable MaybeKafkaBytes where
  deserialize = do
    l <- deserialize :: Get Int32
    case l of
      -1 -> return $ MKB Nothing
      _ -> do 
        bs <- getByteString $ fromIntegral l
        return $ MKB (Just (KBytes bs))

instance (Deserializable a) => Deserializable [a] where
  deserialize = do
    l <- deserialize :: Get Int32
    replicateM (fromIntegral l) deserialize

instance (Deserializable a, Deserializable b) => Deserializable ((,) a b) where
  deserialize = liftM2 (,) deserialize deserialize
instance (Deserializable a, Deserializable b, Deserializable c) => Deserializable ((,,) a b c) where
  deserialize = liftM3 (,,) deserialize deserialize deserialize
instance (Deserializable a, Deserializable b, Deserializable c, Deserializable d) => Deserializable ((,,,) a b c d) where
  deserialize = liftM4 (,,,) deserialize deserialize deserialize deserialize
instance (Deserializable a, Deserializable b, Deserializable c, Deserializable d, Deserializable e) => Deserializable ((,,,,) a b c d e) where
  deserialize = liftM5 (,,,,) deserialize deserialize deserialize deserialize deserialize

instance Deserializable Int64 where deserialize = getWord64be >>= return . fromIntegral
instance Deserializable Int32 where deserialize = getWord32be >>= return . fromIntegral
instance Deserializable Int16 where deserialize = getWord16be >>= return . fromIntegral
instance Deserializable Int8  where deserialize = getWord8    >>= return . fromIntegral

instance IsList ProduceResponse where
  type Item ProduceResponse = (TopicName, [(Partition, KafkaError, Offset)])
  fromList xs = ProduceResp xs
  toList (ProduceResp xs) = xs
instance IsList Replicas where
  type Item Replicas = Int32
  fromList xs = Replicas xs
  toList (Replicas xs) = xs
instance IsList Isr where
  type Item Isr = Int32
  fromList xs = Isr xs
  toList (Isr xs) = xs
instance IsList MessageSet where
  type Item MessageSet = MessageSetMember
  fromList xs = MessageSet xs
  toList (MessageSet xs) = xs
