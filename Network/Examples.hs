{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE NamedFieldPuns #-}

module Network.Examples where

import Control.Monad (liftM)
import qualified Data.ByteString.Char8 as B
import Data.Serialize.Get
import Network
import System.IO

import Network.Kafka.Protocol

{-

Requests for a topic-partition combo need to go to the broker that "owns" that
topic-partition. Normally, you'd fetch metadata about topics you're interested
in and get back a list of brokers and a list of partitions and which broker
"owns" which partition. For simplicity's sake, we only have a single Kafka node
and a single partition, so we'll ignore all of that here.

Start Kafka and add a topic called "test-topic" with a single partition (see
http://kafka.apache.org/documentation.html#quickstart):

./bin/zookeeper-server-start.sh config/zookeeper.properties
./bin/kafka-server-start.sh config/server.properties
./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test-topic

**************************************************

GHCi usage:

:set -XOverloadedStrings
:l Network/Examples.hs

(produceResponse, fetchResponse) <- responses


-}

responses :: IO (Either String Response, Either String Response)
responses = do
  h <- connectTo "localhost" $ PortNumber 9092
  let pr = request "milena-examples" produceRequest
      fr = request "milena-examples" fetchRequest
  produceResponse <- req pr h
  fetchResponse <- req fr h
  _ <- hClose h
  return (produceResponse, fetchResponse)

-- https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Requests
request :: KafkaString -> RequestMessage -> Request
request clientId m = Request ( CorrelationId 0
                             , ClientId clientId
                             , m
                             )

produceRequest :: RequestMessage
produceRequest =
  let topicName = TName (KString "test-topic")
      messageSet = MessageSet [messageSetMember]
      offset = Offset 0 -- ignored when producer is sending to the broker
      messageSetMember = MessageSetMember (offset, m)
      -- check out the protocol doc for info about messages and message sets:
      -- https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets
      m = Message ( Crc 1 -- CRC32 gets computed during serialization
                  , MagicByte 0
                  , Attributes 0
                  , Key (MKB Nothing)
                  , Value (MKB (Just (KBytes "raw message bytes")))
                  )
  -- RequiredAcks requires special attention - see the doc:
  -- https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ProduceAPI
  in ProduceRequest $ ProduceReq ( RequiredAcks 1
                                 , Timeout 10000
                                 , [(topicName, [(Partition 0, messageSet)])]
                                 )

-- https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-FetchAPI
fetchRequest :: RequestMessage
fetchRequest =
  let topicName = TName (KString "test-topic")
  in FetchRequest $ FetchReq ( ReplicaId (-1) -- "client consumers should always specify this as -1"
                             , MaxWaitTime 15000
                             , MinBytes 1
                             , [(topicName, [( Partition 0
                                             , Offset 0
                                             , MaxBytes 10000)])])

req :: Request -> Handle -> IO (Either String Response)
req r h = do
  let bytes = requestBytes r
  B.hPut h bytes
  hFlush h
  let reader = B.hGet h
  rawLength <- reader 4
  case runGet (liftM fromIntegral getWord32be) rawLength of
    Right dataLength -> do
      resp <- reader dataLength
      return $ runGet (getResponse dataLength) resp
    Left s -> return $ Left s
