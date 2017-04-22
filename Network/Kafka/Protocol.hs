{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE Rank2Types #-}

module Network.Kafka.Protocol
  ( module Network.Kafka.Protocol
  ) where

import Control.Applicative
import Control.Category (Category(..))
import Control.Exception (Exception)
import Control.Lens
import Control.Monad (replicateM, liftM2, liftM3, liftM4, liftM5, unless)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Bits ((.&.))
import Data.ByteString.Char8 (ByteString)
import Data.ByteString.Lens (unpackedChars)
import Data.Digest.CRC32
import Data.Int
import Data.Serialize.Get
import Data.Serialize.Put
import GHC.Exts (IsString(..))
import System.IO
import Numeric.Lens
import Prelude hiding ((.), id)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as LB (fromStrict, toStrict)
import qualified Codec.Compression.GZip as GZip (compress, decompress)
import qualified Network

data ReqResp a where
  MetadataRR :: MonadIO m => MetadataRequest -> ReqResp (m MetadataResponse)
  ProduceRR  :: MonadIO m => ProduceRequest  -> ReqResp (m ProduceResponse)
  FetchRR    :: MonadIO m => FetchRequest    -> ReqResp (m FetchResponse)
  OffsetRR   :: MonadIO m => OffsetRequest   -> ReqResp (m OffsetResponse)

doRequest' :: (Deserializable a, MonadIO m) => CorrelationId -> Handle -> Request -> m (Either String a)
doRequest' correlationId h r = do
  rawLength <- liftIO $ do
    B.hPut h $ requestBytes r
    hFlush h
    B.hGet h 4
  case runGet (fmap fromIntegral getWord32be) rawLength of
    Left s -> return $ Left s
    Right dataLength -> do
      responseBytes <- liftIO $ B.hGet h dataLength
      return $ flip runGet responseBytes $ do
        correlationId' <- deserialize
        unless (correlationId == correlationId') $ fail ("Expected " ++ show correlationId ++ " but got " ++ show correlationId')
        isolate (dataLength - 4) deserialize

doRequest :: MonadIO m => ClientId -> CorrelationId -> Handle -> ReqResp (m a) -> m (Either String a)
doRequest clientId correlationId h (MetadataRR req) = doRequest' correlationId h $ Request (correlationId, clientId, MetadataRequest req)
doRequest clientId correlationId h (ProduceRR req)  = doRequest' correlationId h $ Request (correlationId, clientId, ProduceRequest req)
doRequest clientId correlationId h (FetchRR req)    = doRequest' correlationId h $ Request (correlationId, clientId, FetchRequest req)
doRequest clientId correlationId h (OffsetRR req)   = doRequest' correlationId h $ Request (correlationId, clientId, OffsetRequest req)

class Serializable a where
  serialize :: a -> Put

class Deserializable a where
  deserialize :: Get a

newtype GroupCoordinatorResponse = GroupCoordinatorResp (KafkaError, Broker) deriving (Show, Eq, Deserializable)

newtype ApiKey = ApiKey Int16 deriving (Show, Eq, Deserializable, Serializable, Num, Integral, Ord, Real, Enum) -- numeric ID for API (i.e. metadata req, produce req, etc.)
newtype ApiVersion = ApiVersion Int16 deriving (Show, Eq, Deserializable, Serializable, Num, Integral, Ord, Real, Enum)
newtype CorrelationId = CorrelationId Int32 deriving (Show, Eq, Deserializable, Serializable, Num, Integral, Ord, Real, Enum)
newtype ClientId = ClientId KafkaString deriving (Show, Eq, Deserializable, Serializable, IsString)

data RequestMessage = MetadataRequest MetadataRequest
                    | ProduceRequest ProduceRequest
                    | FetchRequest FetchRequest
                    | OffsetRequest OffsetRequest
                    | OffsetCommitRequest OffsetCommitRequest
                    | OffsetFetchRequest OffsetFetchRequest
                    | GroupCoordinatorRequest GroupCoordinatorRequest
                    deriving (Show, Eq)

newtype MetadataRequest = MetadataReq [TopicName] deriving (Show, Eq, Serializable, Deserializable)
newtype TopicName = TName { _tName :: KafkaString } deriving (Eq, Ord, Deserializable, Serializable, IsString)

instance Show TopicName where
  show = show . B.unpack . _kString. _tName

newtype KafkaBytes = KBytes { _kafkaByteString :: ByteString } deriving (Show, Eq, IsString)
newtype KafkaString = KString { _kString :: ByteString } deriving (Show, Eq, Ord, IsString)

newtype ProduceResponse =
  ProduceResp { _produceResponseFields :: [(TopicName, [(Partition, KafkaError, Offset)])] }
  deriving (Show, Eq, Deserializable, Serializable)

newtype OffsetResponse =
  OffsetResp { _offsetResponseFields :: [(TopicName, [PartitionOffsets])] }
  deriving (Show, Eq, Deserializable)

newtype PartitionOffsets =
  PartitionOffsets { _partitionOffsetsFields :: (Partition, KafkaError, [Offset]) }
  deriving (Show, Eq, Deserializable)

newtype FetchResponse =
  FetchResp { _fetchResponseFields :: [(TopicName, [(Partition, KafkaError, Offset, MessageSet)])] }
  deriving (Show, Eq, Serializable, Deserializable)

newtype MetadataResponse = MetadataResp { _metadataResponseFields :: ([Broker], [TopicMetadata]) } deriving (Show, Eq, Deserializable)
newtype Broker = Broker { _brokerFields :: (NodeId, Host, Port) } deriving (Show, Eq, Ord, Deserializable)
newtype NodeId = NodeId { _nodeId :: Int32 } deriving (Show, Eq, Deserializable, Num, Integral, Ord, Real, Enum)
newtype Host = Host { _hostKString :: KafkaString } deriving (Show, Eq, Ord, Deserializable, IsString)
newtype Port = Port { _portInt :: Int32 } deriving (Show, Eq, Deserializable, Num, Integral, Ord, Real, Enum)
newtype TopicMetadata = TopicMetadata { _topicMetadataFields :: (KafkaError, TopicName, [PartitionMetadata]) } deriving (Show, Eq, Deserializable)
newtype PartitionMetadata = PartitionMetadata { _partitionMetadataFields :: (KafkaError, Partition, Leader, Replicas, Isr) } deriving (Show, Eq, Deserializable)
newtype Leader = Leader { _leaderId :: Maybe Int32 } deriving (Show, Eq, Ord)

newtype Replicas = Replicas [Int32] deriving (Show, Eq, Serializable, Deserializable)
newtype Isr = Isr [Int32] deriving (Show, Eq, Deserializable)

newtype OffsetCommitResponse = OffsetCommitResp [(TopicName, [(Partition, KafkaError)])] deriving (Show, Eq, Deserializable)
newtype OffsetFetchResponse = OffsetFetchResp [(TopicName, [(Partition, Offset, Metadata, KafkaError)])] deriving (Show, Eq, Deserializable)

newtype OffsetRequest = OffsetReq (ReplicaId, [(TopicName, [(Partition, Time, MaxNumberOfOffsets)])]) deriving (Show, Eq, Serializable)
newtype Time = Time { _timeInt :: Int64 } deriving (Show, Eq, Serializable, Num, Integral, Ord, Real, Enum, Bounded)
newtype MaxNumberOfOffsets = MaxNumberOfOffsets Int32 deriving (Show, Eq, Serializable, Num, Integral, Ord, Real, Enum)

newtype FetchRequest =
  FetchReq (ReplicaId, MaxWaitTime, MinBytes,
            [(TopicName, [(Partition, Offset, MaxBytes)])])
  deriving (Show, Eq, Deserializable, Serializable)

newtype ReplicaId = ReplicaId Int32 deriving (Show, Eq, Num, Integral, Ord, Real, Enum, Serializable, Deserializable)
newtype MaxWaitTime = MaxWaitTime Int32 deriving (Show, Eq, Num, Integral, Ord, Real, Enum, Serializable, Deserializable)
newtype MinBytes = MinBytes Int32 deriving (Show, Eq, Num, Integral, Ord, Real, Enum, Serializable, Deserializable)
newtype MaxBytes = MaxBytes Int32 deriving (Show, Eq, Num, Integral, Ord, Real, Enum, Serializable, Deserializable)

newtype ProduceRequest =
  ProduceReq (RequiredAcks, Timeout,
              [(TopicName, [(Partition, MessageSet)])])
  deriving (Show, Eq, Serializable)

newtype RequiredAcks =
  RequiredAcks Int16 deriving (Show, Eq, Serializable, Deserializable, Num, Integral, Ord, Real, Enum)
newtype Timeout =
  Timeout Int32 deriving (Show, Eq, Serializable, Deserializable, Num, Integral, Ord, Real, Enum)
newtype Partition =
  Partition Int32 deriving (Show, Eq, Serializable, Deserializable, Num, Integral, Ord, Real, Enum)

data MessageSet = MessageSet { _codec :: CompressionCodec, _messageSetMembers :: [MessageSetMember] }
  deriving (Show, Eq)
data MessageSetMember =
  MessageSetMember { _setOffset :: Offset, _setMessage :: Message } deriving (Show, Eq)

newtype Offset = Offset Int64 deriving (Show, Eq, Serializable, Deserializable, Num, Integral, Ord, Real, Enum)

newtype Message =
  Message { _messageFields :: (Crc, MagicByte, Attributes, Key, Value) }
  deriving (Show, Eq, Deserializable)

data CompressionCodec = NoCompression | Gzip deriving (Show, Eq)

newtype Crc = Crc Int32 deriving (Show, Eq, Serializable, Deserializable, Num, Integral, Ord, Real, Enum)
newtype MagicByte = MagicByte Int8 deriving (Show, Eq, Serializable, Deserializable, Num, Integral, Ord, Real, Enum)
data Attributes = Attributes { _compressionCodec :: CompressionCodec } deriving (Show, Eq)

newtype Key = Key { _keyBytes :: Maybe KafkaBytes } deriving (Show, Eq)
newtype Value = Value { _valueBytes :: Maybe KafkaBytes } deriving (Show, Eq)

data ResponseMessage = MetadataResponse MetadataResponse
                     | ProduceResponse ProduceResponse
                     | FetchResponse FetchResponse
                     | OffsetResponse OffsetResponse
                     | OffsetCommitResponse OffsetCommitResponse
                     | OffsetFetchResponse OffsetFetchResponse
                     | GroupCoordinatorResponse GroupCoordinatorResponse
                     deriving (Show, Eq)

newtype GroupCoordinatorRequest = GroupCoordinatorReq ConsumerGroup deriving (Show, Eq, Serializable)

newtype OffsetCommitRequest = OffsetCommitReq (ConsumerGroup, [(TopicName, [(Partition, Offset, Time, Metadata)])]) deriving (Show, Eq, Serializable)
newtype OffsetFetchRequest = OffsetFetchReq (ConsumerGroup, [(TopicName, [Partition])]) deriving (Show, Eq, Serializable)
newtype ConsumerGroup = ConsumerGroup KafkaString deriving (Show, Eq, Serializable, Deserializable, IsString)
newtype Metadata = Metadata KafkaString deriving (Show, Eq, Serializable, Deserializable, IsString)

errorKafka :: KafkaError -> Int16
errorKafka NoError                             = 0
errorKafka Unknown                             = -1
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

data KafkaError = NoError -- ^ @0@ No error--it worked!
                | Unknown -- ^ @-1@ An unexpected server error
                | OffsetOutOfRange -- ^ @1@ The requested offset is outside the range of offsets maintained by the server for the given topic/partition.
                | InvalidMessage -- ^ @2@ This indicates that a message contents does not match its CRC
                | UnknownTopicOrPartition -- ^ @3@ This request is for a topic or partition that does not exist on this broker.
                | InvalidMessageSize -- ^ @4@ The message has a negative size
                | LeaderNotAvailable -- ^ @5@ This error is thrown if we are in the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes.
                | NotLeaderForPartition -- ^ @6@ This error is thrown if the client attempts to send messages to a replica that is not the leader for some partition. It indicates that the clients metadata is out of date.
                | RequestTimedOut -- ^ @7@ This error is thrown if the request exceeds the user-specified time limit in the request.
                | BrokerNotAvailable -- ^ @8@ This is not a client facing error and is used mostly by tools when a broker is not alive.
                | ReplicaNotAvailable -- ^ @9@ If replica is expected on a broker, but is not.
                | MessageSizeTooLarge -- ^ @10@ The server has a configurable maximum message size to avoid unbounded memory allocation. This error is thrown if the client attempt to produce a message larger than this maximum.
                | StaleControllerEpochCode -- ^ @11@ Internal error code for broker-to-broker communication.
                | OffsetMetadataTooLargeCode -- ^ @12@ If you specify a string larger than configured maximum for offset metadata
                | OffsetsLoadInProgressCode -- ^ @14@ The broker returns this error code for an offset fetch request if it is still loading offsets (after a leader change for that offsets topic partition).
                | ConsumerCoordinatorNotAvailableCode -- ^ @15@ The broker returns this error code for consumer metadata requests or offset commit requests if the offsets topic has not yet been created.
                | NotCoordinatorForConsumerCode -- ^ @16@ The broker returns this error code if it receives an offset fetch or commit request for a consumer group that it is not a coordinator for.
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

instance Exception KafkaError

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
apiKey ProduceRequest{} = ApiKey 0
apiKey FetchRequest{} = ApiKey 1
apiKey OffsetRequest{} = ApiKey 2
apiKey MetadataRequest{} = ApiKey 3
apiKey OffsetCommitRequest{} = ApiKey 8
apiKey OffsetFetchRequest{} = ApiKey 9
apiKey GroupCoordinatorRequest{} = ApiKey 10

instance Serializable RequestMessage where
  serialize (ProduceRequest r) = serialize r
  serialize (FetchRequest r) = serialize r
  serialize (OffsetRequest r) = serialize r
  serialize (MetadataRequest r) = serialize r
  serialize (OffsetCommitRequest r) = serialize r
  serialize (OffsetFetchRequest r) = serialize r
  serialize (GroupCoordinatorRequest r) = serialize r

instance Serializable Int64 where serialize = putWord64be . fromIntegral
instance Serializable Int32 where serialize = putWord32be . fromIntegral
instance Serializable Int16 where serialize = putWord16be . fromIntegral
instance Serializable Int8  where serialize = putWord8    . fromIntegral

instance Serializable Key where
  serialize (Key (Just bs)) = serialize bs
  serialize (Key Nothing)   = serialize (-1 :: Int32)

instance Serializable Value where
  serialize (Value (Just bs)) = serialize bs
  serialize (Value Nothing)   = serialize (-1 :: Int32)

instance Serializable KafkaString where
  serialize (KString bs) = do
    let l = fromIntegral (B.length bs) :: Int16
    serialize l
    putByteString bs

instance Serializable MessageSet where
  serialize (MessageSet codec messageSet) = do
    let bytes = runPut $ mapM_ serialize (compress codec messageSet)
        l = fromIntegral (B.length bytes) :: Int32
    serialize l
    putByteString bytes

    where compress :: CompressionCodec -> [MessageSetMember] -> [MessageSetMember]
          compress NoCompression ms = ms
          compress c ms = [MessageSetMember (Offset (-1)) (message c ms)]

          message :: CompressionCodec -> [MessageSetMember] -> Message
          message c ms = Message (0, 0, Attributes c, Key Nothing, value (compressor c) ms)

          compressor :: CompressionCodec -> (ByteString -> ByteString)
          compressor c = case c of
            Gzip -> LB.toStrict . GZip.compress . LB.fromStrict
            _ -> fail "Unsupported compression codec"

          value :: (ByteString -> ByteString) -> [MessageSetMember] -> Value
          value c ms = Value . Just . KBytes $ c (runPut $ mapM_ serialize ms)

instance Serializable Attributes where
  serialize = serialize . bits
    where bits :: Attributes -> Int8
          bits = codecValue . _compressionCodec

          codecValue :: CompressionCodec -> Int8
          codecValue NoCompression = 0
          codecValue Gzip = 1

instance Serializable KafkaBytes where
  serialize (KBytes bs) = do
    let l = fromIntegral (B.length bs) :: Int32
    serialize l
    putByteString bs

instance Serializable MessageSetMember where
  serialize (MessageSetMember offset msg) = do
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

    decompressed <- mapM decompress ms

    return $ MessageSet NoCompression (concat decompressed)

      where getMembers :: Get [MessageSetMember]
            getMembers = do
              wasEmpty <- isEmpty
              if wasEmpty
              then return []
              else liftM2 (:) deserialize getMembers <|> (remaining >>= getBytes >> return [])

            decompress :: MessageSetMember -> Get [MessageSetMember]
            decompress m = if isCompressed m
                  then decompressSetMember m
                  else return [m]

            isCompressed :: MessageSetMember -> Bool
            isCompressed = messageCompressed . _setMessage

            messageCompressed :: Message -> Bool
            messageCompressed (Message (_, _, att, _, _)) = _compressionCodec att /= NoCompression

            decompressSetMember :: MessageSetMember -> Get [MessageSetMember]
            decompressSetMember (MessageSetMember _ (Message (_, _, att, _, Value v))) = case v of
              Just bytes -> decompressMessage (decompressor att) (_kafkaByteString bytes)
              Nothing -> fail "Expecting a compressed message set, empty data set received"

            decompressor :: Attributes -> (ByteString -> ByteString)
            decompressor att = case _compressionCodec att of
              Gzip -> LB.toStrict . GZip.decompress . LB.fromStrict
              _ -> fail "Unsupported compression codec."

            decompressMessage :: (ByteString -> ByteString) -> ByteString -> Get [MessageSetMember]
            decompressMessage f = getDecompressedMembers . f

            getDecompressedMembers :: ByteString -> Get [MessageSetMember]
            getDecompressedMembers "" = return [] -- a compressed empty message
            getDecompressedMembers val = do
              let res = runGetPartial deserialize val :: Result MessageSetMember
              case res of
                Fail err _ -> fail err
                Partial _ -> fail "Could not consume all available data"
                Done v vv -> fmap (v :) (getDecompressedMembers vv)

instance Deserializable MessageSetMember where
  deserialize = do
    o <- deserialize
    l <- deserialize :: Get Int32
    m <- isolate (fromIntegral l) deserialize
    return $ MessageSetMember o m

instance Deserializable Leader where
  deserialize = do
    x <- deserialize :: Get Int32
    let l = Leader $ if x == -1 then Nothing else Just x
    return l

instance Deserializable Attributes where
  deserialize = do
    i <- deserialize :: Get Int8
    codec <- case compressionCodecFromValue i of
      Just c -> return c
      Nothing -> fail $ "Unknown compression codec value found in: " ++ show i
    return $ Attributes codec

compressionCodecFromValue :: Int8 -> Maybe CompressionCodec
compressionCodecFromValue i | eq 1 = Just Gzip
                            | eq 0 = Just NoCompression
                            | otherwise = Nothing
                            where eq y = i .&. y == y

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

instance Deserializable Key where
  deserialize = do
    l <- deserialize :: Get Int32
    case l of
      -1 -> return (Key Nothing)
      _ -> do
        bs <- getByteString $ fromIntegral l
        return $ Key (Just (KBytes bs))

instance Deserializable Value where
  deserialize = do
    l <- deserialize :: Get Int32
    case l of
      -1 -> return (Value Nothing)
      _ -> do
        bs <- getByteString $ fromIntegral l
        return $ Value (Just (KBytes bs))

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

instance Deserializable Int64 where deserialize = fmap fromIntegral getWord64be
instance Deserializable Int32 where deserialize = fmap fromIntegral getWord32be
instance Deserializable Int16 where deserialize = fmap fromIntegral getWord16be
instance Deserializable Int8  where deserialize = fmap fromIntegral getWord8

-- * Generated lenses

makeLenses ''TopicName

makeLenses ''KafkaBytes
makeLenses ''KafkaString

makeLenses ''ProduceResponse

makeLenses ''OffsetResponse
makeLenses ''PartitionOffsets

makeLenses ''FetchResponse

makeLenses ''MetadataResponse
makeLenses ''Broker
makeLenses ''NodeId
makeLenses ''Host
makeLenses ''Port
makeLenses ''TopicMetadata
makeLenses ''PartitionMetadata
makeLenses ''Leader

makeLenses ''Time

makeLenses ''Partition

makeLenses ''MessageSet
makeLenses ''MessageSetMember
makeLenses ''Offset

makeLenses ''Message

makeLenses ''Key
makeLenses ''Value

makePrisms ''ResponseMessage

-- * Composed lenses

keyed :: (Field1 a a b b, Choice p, Applicative f, Eq b) => b -> Optic' p f a a
keyed k = filtered (view $ _1 . to (== k))

metadataResponseBrokers :: Lens' MetadataResponse [Broker]
metadataResponseBrokers = metadataResponseFields . _1

topicsMetadata :: Lens' MetadataResponse [TopicMetadata]
topicsMetadata = metadataResponseFields . _2

topicMetadataKafkaError :: Lens' TopicMetadata KafkaError
topicMetadataKafkaError = topicMetadataFields . _1

topicMetadataName :: Lens' TopicMetadata TopicName
topicMetadataName = topicMetadataFields . _2

partitionsMetadata :: Lens' TopicMetadata [PartitionMetadata]
partitionsMetadata = topicMetadataFields . _3

partitionId :: Lens' PartitionMetadata Partition
partitionId = partitionMetadataFields . _2

partitionMetadataLeader :: Lens' PartitionMetadata Leader
partitionMetadataLeader = partitionMetadataFields . _3

brokerNode :: Lens' Broker NodeId
brokerNode = brokerFields . _1

brokerHost :: Lens' Broker Host
brokerHost = brokerFields . _2

brokerPort :: Lens' Broker Port
brokerPort = brokerFields . _3

fetchResponseMessages :: Fold FetchResponse MessageSet
fetchResponseMessages = fetchResponseFields . folded . _2 . folded . _4

fetchResponseByTopic :: TopicName -> Fold FetchResponse (Partition, KafkaError, Offset, MessageSet)
fetchResponseByTopic t = fetchResponseFields . folded . keyed t . _2 . folded

messageSetByPartition :: Partition -> Fold (Partition, KafkaError, Offset, MessageSet) MessageSetMember
messageSetByPartition p = keyed p . _4 . messageSetMembers . folded

fetchResponseMessageMembers :: Fold FetchResponse MessageSetMember
fetchResponseMessageMembers = fetchResponseMessages . messageSetMembers . folded

messageKey :: Lens' Message Key
messageKey = messageFields . _4

messageKeyBytes :: Fold Message ByteString
messageKeyBytes = messageKey . keyBytes . folded . kafkaByteString

messageValue :: Lens' Message Value
messageValue = messageFields . _5

payload :: Fold Message ByteString
payload = messageValue . valueBytes . folded . kafkaByteString

offsetResponseOffset :: Partition -> Fold OffsetResponse Offset
offsetResponseOffset p = offsetResponseFields . folded . _2 . folded . partitionOffsetsFields . keyed p . _3 . folded

messageSet :: Partition -> TopicName -> Fold FetchResponse MessageSetMember
messageSet p t = fetchResponseByTopic t . messageSetByPartition p

nextOffset :: Lens' MessageSetMember Offset
nextOffset = setOffset . adding 1

findPartitionMetadata :: Applicative f => TopicName -> LensLike' f TopicMetadata [PartitionMetadata]
findPartitionMetadata t = filtered (view $ topicMetadataName . to (== t)) . partitionsMetadata

findPartition :: Partition -> Prism' PartitionMetadata PartitionMetadata
findPartition p = filtered (view $ partitionId . to (== p))

hostString :: Lens' Host String
hostString = hostKString . kString . unpackedChars

portId :: IndexPreservingGetter Port Network.PortID
portId = portInt . to fromIntegral . to Network.PortNumber
