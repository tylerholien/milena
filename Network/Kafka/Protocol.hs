{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TypeFamilies #-}

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

-- cmdr :: RequestMessage
-- cmdr = ConsumerMetadataRequest $ ConsumerMetadataReq "group"

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
fetchRequest = FetchRequest $ FetchReq (-1, 15000, 1, [("test", [(0, Offset 104, 10000)]), ("example", [(0, 4, 10000)])])

produceRequest :: Key -> Value -> RequestMessage
produceRequest k v = ProduceRequest $ ProduceReq (1, 10000, [("test", [(0, [MessageSetMember (0, (Message (1, 0, 0, k, v)))])])])

metadataRequest :: RequestMessage
metadataRequest = MetadataRequest $ MetadataReq ["test", "example"]

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
                   <|> liftM FetchResponse        (isolate l deserialize)
                   <|> liftM ProduceResponse      (isolate l deserialize)
                   <|> liftM OffsetCommitResponse (isolate l deserialize)
                   <|> liftM OffsetFetchResponse  (isolate l deserialize)

requestBytes :: RequestMessage -> ByteString
requestBytes x = runPut $ do
  putWord32be . fromIntegral $ B.length mr
  putByteString mr
    where mr = runPut $ serialize x

apiKey :: RequestMessage -> ApiKey
apiKey (ProduceRequest{}) = ApiKey 0
apiKey (FetchRequest{}) = ApiKey 1
apiKey (OffsetRequest{}) = ApiKey 2
apiKey (MetadataRequest{}) = ApiKey 3
apiKey (OffsetCommitRequest{}) = ApiKey 8
apiKey (OffsetFetchRequest{}) = ApiKey 9
-- apiKey (ConsumerMetadataRequest{}) = ApiKey 10

serializeRequest :: RequestMessage -> Put
serializeRequest (ProduceRequest r) = serialize r
serializeRequest (FetchRequest r) = serialize r
serializeRequest (OffsetRequest r) = serialize r
serializeRequest (MetadataRequest r) = serialize r
serializeRequest (OffsetCommitRequest r) = serialize r
serializeRequest (OffsetFetchRequest r) = serialize r
-- serializeRequest (ConsumerMetadataRequest r) = serialize r

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
newtype TopicName = TopicName KafkaString deriving (Show, Eq, Deserializable, Serializable, IsString)

newtype KafkaBytes = KBytes ByteString deriving (Show, Eq, IsString)
newtype KafkaString = KString ByteString deriving (Show, Eq, IsString)

newtype ProduceResponse = ProduceResp [(TopicName, [(Partition, ErrorCode, Offset)])] deriving (Show, Eq, Deserializable, Serializable)

newtype OffsetResponse = OffsetResp [(TopicName, [PartitionOffsets])] deriving (Show, Eq, Deserializable)
newtype PartitionOffsets = PartitionOffsets (Partition, ErrorCode, [Offset]) deriving (Show, Eq, Deserializable)

newtype FetchResponse = FetchResp [(TopicName, [(Partition, ErrorCode, Offset, MessageSet)])] deriving (Show, Eq, Serializable, Deserializable)

newtype ErrorCode = ErrorCode Int16 deriving (Show, Eq, Serializable, Deserializable, Num)

newtype MetadataResponse = MetadataResp ([Broker], [TopicMetadata]) deriving (Show, Eq, Deserializable)
newtype Broker = Broker (NodeId, Host, Port) deriving (Show, Eq, Deserializable)
newtype NodeId = NodeId Int32 deriving (Show, Eq, Deserializable, Num)
newtype Host = Host KafkaString deriving (Show, Eq, Deserializable, IsString)
newtype Port = Port Int32 deriving (Show, Eq, Deserializable, Num)
newtype TopicMetadata = TopicMetadata (TopicErrorCode, TopicName, [PartitionMetadata]) deriving (Show, Eq, Deserializable)
newtype TopicErrorCode = TopicErrorCode Int16 deriving (Show, Eq, Deserializable, Num)
newtype PartitionMetadata = PartitionMetadata (PartitionErrorCode, PartitionId, Leader, Replicas, Isr) deriving (Show, Eq, Deserializable)
newtype PartitionErrorCode = PartitionErrorCode Int16 deriving (Show, Eq, Deserializable, Num)
newtype PartitionId = PartitionId Int32 deriving (Show, Eq, Deserializable, Num)
newtype Leader = Leader Int32 deriving (Show, Eq, Deserializable, Num)
newtype Replicas = Replicas [Int32] deriving (Show, Eq, Serializable, Deserializable)
newtype Isr = Isr [Int32] deriving (Show, Eq, Deserializable)

newtype OffsetCommitResponse = OffsetCommitResp [(TopicName, [(Partition, ErrorCode)])] deriving (Show, Eq, Deserializable)
newtype OffsetFetchResponse = OffsetFetchResp [(TopicName, [(Partition, Offset, Metadata, ErrorCode)])] deriving (Show, Eq, Deserializable)

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

-- omginstances...

instance Serializable RequestMessage where
  serialize r = do
    serialize (apiKey r)
    serialize (ApiVersion 0)
    serialize (CorrelationId 1)
    serialize (ClientId "poseidon")
    serializeRequest r

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
  else do m <- deserialize
          ms <- getMembers
          return (m:ms)

instance Deserializable MessageSetMember where
  deserialize = do
    o <- deserialize
    l <- deserialize :: Get Int32
    m <- isolate (fromIntegral l) deserialize
    return $ MessageSetMember (o, m)

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
  type Item ProduceResponse = (TopicName, [(Partition, ErrorCode, Offset)])
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
