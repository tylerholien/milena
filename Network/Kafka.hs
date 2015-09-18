{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleContexts #-}

module Network.Kafka where

import Control.Applicative
import Control.Exception (IOException)
import Control.Exception.Lifted (catch)
import Control.Lens
import Control.Monad (liftM)
import Control.Monad.Trans (liftIO, lift)
import Control.Monad.Trans.Either
import Control.Monad.Trans.State
import Data.ByteString.Char8 (ByteString)
import Data.List.NonEmpty (NonEmpty(..))
import qualified Data.List.NonEmpty as NE
import Data.Monoid ((<>))
import qualified Data.Pool as Pool
import Data.Serialize.Get
import System.IO
import qualified Data.ByteString.Char8 as B
import qualified Data.Map as M
import qualified Network
import Prelude

import Network.Kafka.Protocol

type KafkaAddress = (Host, Port)

data KafkaState = KafkaState { -- | Name to use as a client ID.
                               _stateName :: KafkaString
                               -- | How many acknowledgements are required for producing.
                             , _stateRequiredAcks :: RequiredAcks
                               -- | Time in milliseconds to wait for messages to be produced by broker.
                             , _stateRequestTimeout :: Timeout
                               -- | Minimum size of response bytes to block for.
                             , _stateWaitSize :: MinBytes
                               -- | Maximum size of response bytes to retrieve.
                             , _stateBufferSize :: MaxBytes
                               -- | Maximum time in milliseconds to wait for response.
                             , _stateWaitTime :: MaxWaitTime
                               -- | An incrementing counter of requests.
                             , _stateCorrelationId :: CorrelationId
                               -- | Broker cache
                             , _stateBrokers :: M.Map Leader Broker
                               -- | Connection cache
                             , _stateConnections :: M.Map KafkaAddress (Pool.Pool Handle)
                               -- | Topic metadata cache
                             , _stateTopicMetadata :: M.Map TopicName TopicMetadata
                             , _stateAddresses :: NonEmpty KafkaAddress
                             }

makeLenses ''KafkaState

-- | The core Kafka monad.
type Kafka = StateT KafkaState (EitherT KafkaClientError IO)

type KafkaClientId = KafkaString

-- | Errors given from the Kafka monad.
data KafkaClientError = -- | A response did not contain an offset.
                        KafkaNoOffset
                        -- | Got a different form of a response than was requested.
                      | KafkaExpected KafkaExpectedResponse
                        -- | A value could not be deserialized correctly.
                      | KafkaDeserializationError String -- TODO: cereal is Stringly typed, should use tickle
                        -- | Could not find a cached broker for the found leader.
                      | KafkaInvalidBroker Leader
                      | KafkaFailedToFetchMetadata
                      | KafkaIOException IOException
                        deriving (Eq, Show)

-- | Type of response to expect, used for 'KafkaExpected' error.
data KafkaExpectedResponse = ExpectedMetadata
                           | ExpectedFetch
                           | ExpectedProduce
                             deriving (Eq, Show)

-- | An abstract form of Kafka's time. Used for querying offsets.
data KafkaTime = -- | The latest time on the broker.
                 LatestTime
                 -- | The earliest time on the broker.
               | EarliestTime
                 -- | A specific time.
               | OtherTime Time

data PartitionAndLeader = PartitionAndLeader { _palTopic :: TopicName
                                             , _palPartition :: Partition
                                             , _palLeader :: Leader
                                             }
                                             deriving (Show)

makeLenses ''PartitionAndLeader

data TopicAndPartition = TopicAndPartition { _tapTopic :: TopicName
                                           , _tapPartition :: Partition
                                           }
                         deriving (Eq, Ord, Show)

-- | A topic with a serializable message.
data TopicAndMessage = TopicAndMessage { _tamTopic :: TopicName
                                       , _tamMessage :: Message
                                       }
                       deriving (Eq, Show)

makeLenses ''TopicAndMessage

-- | Get the bytes from the Kafka message, ignoring the topic.
tamPayload :: TopicAndMessage -> ByteString
tamPayload = foldOf (tamMessage . payload)

-- * Configuration

-- | Default: @0@
defaultCorrelationId :: CorrelationId
defaultCorrelationId = 0

-- | Default: @1@
defaultRequiredAcks :: RequiredAcks
defaultRequiredAcks = 1

-- | Default: @10000@
defaultRequestTimeout :: Timeout
defaultRequestTimeout = 10000

-- | Default: @0@
defaultMinBytes :: MinBytes
defaultMinBytes = MinBytes 0

-- | Default: @1024 * 1024@
defaultMaxBytes :: MaxBytes
defaultMaxBytes = 1024 * 1024

-- | Default: @0@
defaultMaxWaitTime :: MaxWaitTime
defaultMaxWaitTime = 0

-- | Create a consumer using default values.
mkKafkaState :: KafkaClientId -> KafkaAddress -> KafkaState
mkKafkaState cid addy =
    KafkaState cid
               defaultRequiredAcks
               defaultRequestTimeout
               defaultMinBytes
               defaultMaxBytes
               defaultMaxWaitTime
               defaultCorrelationId
               M.empty
               M.empty
               M.empty
               (addy :| [])

addKafkaAddress :: KafkaAddress -> KafkaState -> KafkaState
addKafkaAddress = over stateAddresses . NE.nub .: cons
  where infixr 9 .:
        (.:) :: (c -> d) -> (a -> b -> c) -> a -> b -> d
        (.:) = (.).(.)

-- | Run the underlying Kafka monad.
runKafka :: KafkaState -> Kafka a -> IO (Either KafkaClientError a)
runKafka s k = runEitherT $ evalStateT k s

-- | Make a request, incrementing the `_stateCorrelationId`.
makeRequest :: RequestMessage -> Kafka Request
makeRequest m = do
  corid <- use stateCorrelationId
  stateCorrelationId += 1
  conid <- use stateName
  return $ Request (corid, ClientId conid, m)

-- | Perform a request and deserialize the response.
doRequest :: Request -> Kafka Response
doRequest r = withAnyHandle $ flip doRequest' r

-- | Catch 'IOException's and wrap them in 'KafkaIOException's.
tryKafkaIO :: IO a -> Kafka a
tryKafkaIO = tryKafka . liftIO

-- | Catch 'IOException's and wrap them in 'KafkaIOException's.
tryKafka :: Kafka a -> Kafka a
tryKafka = (`catch` \e -> lift . left $ KafkaIOException (e :: IOException))

doRequest' :: Handle -> Request -> Kafka Response
doRequest' h r = do
  rawLength <- tryKafkaIO $ do
    B.hPut h $ requestBytes r
    hFlush h
    B.hGet h 4
  dataLength <- runGetKafka (liftM fromIntegral getWord32be) rawLength
  resp <- tryKafkaIO $ B.hGet h dataLength
  runGetKafka (getResponse dataLength) resp

runGetKafka :: Get a -> ByteString -> Kafka a
runGetKafka g bs = lift $ bimapEitherT KafkaDeserializationError id $ hoistEither $ runGet g bs

-- | Send a metadata request
metadata :: MetadataRequest -> Kafka MetadataResponse
metadata request = withAnyHandle $ flip metadata' request

-- | Send a metadata request
metadata' :: Handle -> MetadataRequest -> Kafka MetadataResponse
metadata' h request =
    makeRequest (MetadataRequest request) >>= doRequest' h >>= expectResponse ExpectedMetadata _MetadataResponse

-- | Function to give an error when the response seems wrong.
expectResponse :: KafkaExpectedResponse -> Getting (Leftmost b) ResponseMessage b -> Response -> Kafka b
expectResponse e p = lift . maybe (left $ KafkaExpected e) return . firstOf (responseMessage . p)

-- | Convert an abstract time to a serializable protocol value.
protocolTime :: KafkaTime -> Time
protocolTime LatestTime = Time (-1)
protocolTime EarliestTime = Time (-2)
protocolTime (OtherTime o) = o

-- * Fetching

-- | Default: @-1@
ordinaryConsumerId :: ReplicaId
ordinaryConsumerId = ReplicaId (-1)

-- | Construct a fetch request from the values in the state.
fetchRequest :: Offset -> Partition -> TopicName -> Kafka FetchRequest
fetchRequest o p topic = do
  wt <- use stateWaitTime
  ws <- use stateWaitSize
  bs <- use stateBufferSize
  return $ FetchReq (ordinaryConsumerId, wt, ws, [(topic, [(p, o, bs)])])

-- | Execute a fetch request and get the raw fetch response.
fetch :: FetchRequest -> Kafka FetchResponse
fetch request =
    makeRequest (FetchRequest request) >>= doRequest >>= expectResponse ExpectedFetch _FetchResponse

-- | Extract out messages with their topics from a fetch response.
fetchMessages :: FetchResponse -> [TopicAndMessage]
fetchMessages fr = (fr ^.. fetchResponseFields . folded) >>= tam
    where tam a = TopicAndMessage (a ^. _1) <$> a ^.. _2 . folded . _4 . messageSetMembers . folded . setMessage

updateMetadatas :: [TopicName] -> Kafka ()
updateMetadatas ts = do
  md <- metadata $ MetadataReq ts
  let (brokers, tmds) = (md ^.. metadataResponseBrokers . folded, md ^.. topicsMetadata . folded)
      addresses = map broker2address brokers
  stateAddresses %= NE.nub . NE.fromList . (++ addresses) . NE.toList
  stateBrokers %= \m -> foldr addBroker m brokers
  stateTopicMetadata %= \m -> foldr addTopicMetadata m tmds
  return ()
    where addBroker :: Broker -> M.Map Leader Broker -> M.Map Leader Broker
          addBroker b = M.insert (Leader . Just $ b ^. brokerNode . nodeId) b
          addTopicMetadata :: TopicMetadata -> M.Map TopicName TopicMetadata -> M.Map TopicName TopicMetadata
          addTopicMetadata tm = M.insert (tm ^. topicMetadataName) tm

updateMetadata :: TopicName -> Kafka ()
updateMetadata t = updateMetadatas [t]

updateAllMetadata :: Kafka ()
updateAllMetadata = updateMetadatas []

-- | Execute a handler action, creating a new Pool and updating the connections Map if needed.
withBrokerHandle :: Broker -> (Handle -> Kafka a) -> Kafka a
withBrokerHandle broker = withAddressHandle (broker2address broker)

withAddressHandle :: KafkaAddress -> (Handle -> Kafka a) -> Kafka a
withAddressHandle address kafkaAction = do
  conns <- use stateConnections
  let foundPool = conns ^. at address
  pool <- case foundPool of
    Nothing -> do
      newPool <- tryKafkaIO $ mkPool address
      stateConnections .= (at address ?~ newPool $ conns)
      return newPool
    Just p -> return p
  tryKafka $ Pool.withResource pool kafkaAction
    where
      mkPool :: KafkaAddress -> IO (Pool.Pool Handle)
      mkPool a = Pool.createPool (createHandle a) hClose 1 10 1
        where createHandle (h, p) = Network.connectTo (h ^. hostString) (p ^. portId)

broker2address :: Broker -> KafkaAddress
broker2address broker = (,) (broker ^. brokerHost) (broker ^. brokerPort)

withAnyHandle :: (Handle -> Kafka a) -> Kafka a
withAnyHandle f = do
  (addy :| _) <- use stateAddresses
  x <- withAddressHandle addy f
  stateAddresses %= rotate
  return x
    where rotate :: NonEmpty a -> NonEmpty a
          rotate = NE.fromList . rotate' 1 . NE.toList
          rotate' n xs = zipWith const (drop n (cycle xs)) xs

-- * Offsets

-- | Fields to construct an offset request, per topic and partition.
data PartitionOffsetRequestInfo =
    PartitionOffsetRequestInfo { -- | Time to find an offset for.
                                 _kafkaTime :: KafkaTime
                                 -- | Number of offsets to retrieve.
                               , _maxNumOffsets :: MaxNumberOfOffsets
                               }

-- TODO: Properly look up the offset via the partition.
-- | Get the first found offset.
getLastOffset :: KafkaTime -> Partition -> TopicName -> Kafka Offset
getLastOffset m p t =
    makeRequest (offsetRequest [(TopicAndPartition t p, PartitionOffsetRequestInfo m 1)]) >>= doRequest >>= maybe (StateT . const $ left KafkaNoOffset) return . firstOf (responseMessage . _OffsetResponse . offsetResponseOffset p)

-- | Create an offset request.
offsetRequest :: [(TopicAndPartition, PartitionOffsetRequestInfo)] -> RequestMessage
offsetRequest ts =
    OffsetRequest $ OffsetReq (ReplicaId (-1), M.toList . M.unionsWith (<>) $ fmap f ts)
        where f (TopicAndPartition t p, i) = M.singleton t [g p i]
              g p (PartitionOffsetRequestInfo kt mno) = (p, protocolTime kt, mno)
