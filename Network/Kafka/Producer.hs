module Network.Kafka.Producer where

import Control.Applicative
import Control.Lens
import Control.Monad.Trans (liftIO, lift)
import Control.Monad.Trans.Either
import Data.ByteString.Char8 (ByteString)
import qualified Data.Digest.Murmur32 as Murmur32
import Data.List.Safe ((!!))
import Data.Monoid ((<>))
import System.IO
import qualified Data.Map as M
import System.Random (getStdRandom, randomR)
import Prelude hiding ((!!))

import Network.Kafka
import Network.Kafka.Protocol

-- * Producing

-- | Execute a produce request and get the raw preduce response.
produce :: Handle -> ProduceRequest -> Kafka ProduceResponse
produce handle request =
    makeRequest (ProduceRequest request) >>= doRequest' handle >>= expectResponse ExpectedProduce _ProduceResponse

-- | Construct a produce request with explicit arguments.
produceRequest :: RequiredAcks -> Timeout -> [(TopicAndPartition, MessageSet)] -> ProduceRequest
produceRequest ra ti ts =
    ProduceReq (ra, ti, M.toList . M.unionsWith (<>) $ fmap f ts)
        where f (TopicAndPartition t p, i) = M.singleton t [(p, i)]

-- | Send messages to partition calculated by 'partitionAndCollate'.
produceMessages :: [TopicAndMessage] -> Kafka [ProduceResponse]
produceMessages tams = do
  m <- fmap (fmap groupMessagesToSet) <$> partitionAndCollate tams
  mapM (uncurry send) $ fmap M.toList <$> M.toList m

-- | Create a protocol message set from a list of messages.
groupMessagesToSet :: [TopicAndMessage] -> MessageSet
groupMessagesToSet xs = MessageSet $ msm <$> xs
    where msm = MessageSetMember (Offset (-1)) . _tamMessage

-- | Group messages together with the leader they should be sent to.
partitionAndCollate :: [TopicAndMessage] -> Kafka (M.Map Leader (M.Map TopicAndPartition [TopicAndMessage]))
partitionAndCollate ks = recurse ks M.empty
      where recurse [] accum = return accum
            recurse (x:xs) accum = do
              topicPartitionsList <- brokerPartitionInfo $ _tamTopic x
              let maybeKey = x ^. tamMessage . messageKey . keyBytes
              pal <- case maybeKey of
                Nothing -> getRandPartition topicPartitionsList
                Just key -> return $ getPartitionByKey (_kafkaByteString key) topicPartitionsList
              let leader = maybe (Leader Nothing) _palLeader pal
                  tp = TopicAndPartition <$> pal ^? folded . palTopic <*> pal ^? folded . palPartition
                  b = M.singleton leader $ maybe M.empty (`M.singleton` [x]) tp
                  accum' = M.unionWith (M.unionWith (<>)) accum b
              recurse xs accum'

-- | Compute the partition for a record. This matches the way the official
-- | clients compute the partition.
getPartitionByKey :: ByteString -> [PartitionAndLeader] -> Maybe PartitionAndLeader
getPartitionByKey key ps = let i = Murmur32.asWord32 $ Murmur32.hash32WithSeed 0x9747b28c key
                           in ps !! i

-- | Execute a produce request using the values in the state.
send :: Leader -> [(TopicAndPartition, MessageSet)] -> Kafka ProduceResponse
send l ts = do
  let s = kafkaClientState . stateBrokers . at l
      topicNames = map (_tapTopic . fst) ts
  broker <- findMetadataOrElse topicNames s (KafkaInvalidBroker l)
  requiredAcks <- use (kafkaClientState . stateRequiredAcks)
  requestTimeout <- use (kafkaClientState . stateRequestTimeout)
  withBrokerHandle broker $ \handle -> produce handle $ produceRequest requiredAcks requestTimeout ts

-- | Find a leader and partition for the topic.
brokerPartitionInfo :: TopicName -> Kafka [PartitionAndLeader]
brokerPartitionInfo t = do
  let s = kafkaClientState . stateTopicMetadata . at t
  tmd <- findMetadataOrElse [t] s KafkaFailedToFetchMetadata
  return $ pal <$> tmd ^. partitionsMetadata
    where pal d = PartitionAndLeader t (d ^. partitionId) (d ^. partitionMetadataLeader)

findMetadataOrElse :: [TopicName] -> Getting (Maybe a) KafkaClient (Maybe a) -> KafkaClientError -> Kafka a
findMetadataOrElse ts s err = do
  maybeFound <- use s
  case maybeFound of
    Just x -> return x
    Nothing -> do
      updateMetadatas ts
      maybeFound' <- use s
      case maybeFound' of
        Just x -> return x
        Nothing -> lift $ left $ err

getRandPartition :: [PartitionAndLeader] -> Kafka (Maybe PartitionAndLeader)
getRandPartition ps =
    liftIO $ (ps' ^?) . element <$> getStdRandom (randomR (0, length ps' - 1))
        where ps' = ps ^.. folded . filtered (has $ palLeader . leaderId . _Just)

-- * Messages

-- | Default: @1@
defaultMessageCrc :: Crc
defaultMessageCrc = 1

-- | Default: @0@
defaultMessageMagicByte :: MagicByte
defaultMessageMagicByte = 0

-- | Default: @Nothing@
defaultMessageKey :: Key
defaultMessageKey = Key Nothing

-- | Default: @0@
defaultMessageAttributes :: Attributes
defaultMessageAttributes = 0

-- | Construct a message from a string of bytes using default attributes.
makeMessage :: ByteString -> Message
makeMessage m = Message (defaultMessageCrc, defaultMessageMagicByte, defaultMessageAttributes, defaultMessageKey, Value (Just (KBytes m)))
