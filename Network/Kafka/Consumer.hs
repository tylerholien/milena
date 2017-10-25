module Network.Kafka.Consumer where

import Control.Applicative
import Control.Lens
import System.IO
import Prelude

import Network.Kafka
import Network.Kafka.Protocol

-- * Fetching

-- | Default: @-1@
ordinaryConsumerId :: ReplicaId
ordinaryConsumerId = ReplicaId (-1)

-- | Construct a fetch request from the values in the state.
fetchRequest :: Kafka m => Offset -> Partition -> TopicName -> m FetchRequest
fetchRequest o p topic = do
  wt <- use stateWaitTime
  ws <- use stateWaitSize
  bs <- use stateBufferSize
  return $ FetchReq (ordinaryConsumerId, wt, ws, [(topic, [(p, o, bs)])])

-- | Execute a fetch request and get the raw fetch response.
fetch' :: Kafka m => Handle -> FetchRequest -> m FetchResponse
fetch' h request = makeRequest h $ FetchRR request

fetch :: Kafka m => Offset -> Partition -> TopicName -> m FetchResponse
fetch o p topic = do
  broker <- getTopicPartitionLeader topic p
  withBrokerHandle broker (\handle -> fetch' handle =<< fetchRequest o p topic)

-- | Extract out messages with their topics from a fetch response.
fetchMessages :: FetchResponse -> [TopicAndMessage]
fetchMessages fr = (fr ^.. fetchResponseFields . folded) >>= tam
    where tam a = TopicAndMessage (a ^. _1) <$> a ^.. _2 . folded . _4 . messageSetMembers . folded . setMessage
