module Network.Kafka.Producer where

import Data.Bits ((.&.))
import Data.ByteString.Char8 (ByteString)
import qualified Data.Digest.Murmur32 as Murmur32
import Control.Applicative
import Control.Lens
import Control.Monad.Trans (liftIO)
import Data.Monoid ((<>))
import Data.Set (Set)
import qualified Data.Set as Set
import System.IO
import qualified Data.Map as M
import System.Random (getStdRandom, randomR)

import Prelude

import Network.Kafka
import Network.Kafka.Protocol

-- * Producing

-- | Execute a produce request and get the raw produce response.
produce :: Handle -> ProduceRequest -> Kafka ProduceResponse
produce handle request = makeRequest handle $ ProduceRR request

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
getPartitionByKey :: ByteString -> Set PartitionAndLeader -> Maybe PartitionAndLeader
getPartitionByKey key ps = Set.toAscList ps ^? ix i
  where murmur = Murmur32.asWord32 . Murmur32.hash32WithSeed 0x9747b28c
        toPositive = (.&. 0x7fffffff)
        numPartitions = length ps
        x = fromIntegral $ toPositive $ murmur key
        i = x `mod` numPartitions

-- | Execute a produce request using the values in the state.
send :: Leader -> [(TopicAndPartition, MessageSet)] -> Kafka ProduceResponse
send l ts = do
  let s = stateBrokers . at l
      topicNames = map (_tapTopic . fst) ts
  broker <- findMetadataOrElse topicNames s (KafkaInvalidBroker l)
  requiredAcks <- use stateRequiredAcks
  requestTimeout <- use stateRequestTimeout
  withBrokerHandle broker $ \handle -> produce handle $ produceRequest requiredAcks requestTimeout ts

getRandPartition :: Set PartitionAndLeader -> Kafka (Maybe PartitionAndLeader)
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

-- | Construct a message from a string of bytes using default attributes.
makeKeyedMessage :: ByteString -> ByteString -> Message
makeKeyedMessage k m = Message (defaultMessageCrc, defaultMessageMagicByte, defaultMessageAttributes, Key (Just (KBytes k)), Value (Just (KBytes m)))
