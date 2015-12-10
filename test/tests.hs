{-# LANGUAGE OverloadedStrings #-}

module Main where

import Data.Functor
import Data.Either (isRight, isLeft)
import qualified Data.List.NonEmpty as NE
import Control.Concurrent (threadDelay)
import Control.Lens
import Control.Monad.Except (catchError, throwError)
import Control.Monad.Trans (liftIO)
import Network.Kafka
import Network.Kafka.Consumer
import Network.Kafka.Group
import Network.Kafka.Producer
-- import Network.Kafka.Protocol (JoinGroupRequest(..), GroupCoordinatorResponse(..), GroupCoordinatorRequest(..), ProduceResponse(..), KafkaError(..))
import Test.Tasty
import Test.Tasty.Hspec
import Test.Tasty.QuickCheck
import qualified Data.ByteString.Char8 as B

import Network.Kafka.Protocol
import Data.Int
import Data.Serialize.Put

import Prelude

main :: IO ()
main = testSpec "the specs" specs >>= defaultMain

specs :: Spec
specs = do
  let topic = "milena-test"
      run = runKafka $ mkKafkaState "milena-test-client" ("localhost", 9092)
      -- requireAllAcks = do
      --   stateRequiredAcks .= -1
      --   stateWaitSize .= 1
      --   stateWaitTime .= 1000
      -- byteMessages = fmap (TopicAndMessage topic . makeMessage . B.pack)

  -- describe "can talk to local Kafka server" $ do
    -- prop "can produce messages" $ \ms -> do
      -- result <- run . produceMessages $ byteMessages ms
      -- result `shouldSatisfy` isRight

    -- prop "can produce multiple messages" $ \(ms, ms') -> do
      -- result <- run $ do
      --   r1 <- produceMessages $ byteMessages ms
      --   r2 <- produceMessages $ byteMessages ms'
      --   return $ r1 ++ r2
      -- result `shouldSatisfy` isRight

    -- prop "can fetch messages" $ do
      -- result <- run $ do
      --   offset <- getLastOffset EarliestTime 0 topic
      --   withAnyHandle (\handle -> fetch' handle =<< fetchRequest offset 0 topic)
      -- result `shouldSatisfy` isRight

    -- prop "can roundtrip messages" $ \ms key -> do
      -- let messages = byteMessages ms
      -- result <- run $ do
      --   requireAllAcks
      --   info <- brokerPartitionInfo topic
      --   let Just PartitionAndLeader { _palLeader = leader, _palPartition = partition } = getPartitionByKey (B.pack key) info
      --       payload = [(TopicAndPartition topic partition, groupMessagesToSet messages)]
      --       s = stateBrokers . at leader
      --   [(_topicName, [(_, NoError, offset)])] <- _produceResponseFields <$> send leader payload
      --   broker <- findMetadataOrElse [topic] s (KafkaInvalidBroker leader)
      --   resp <- withBrokerHandle broker (\handle -> fetch' handle =<< fetchRequest offset partition topic)
      --   return $ fmap tamPayload . fetchMessages $ resp
      -- result `shouldBe` Right (tamPayload <$> messages)

    -- prop "can roundtrip keyed messages" $ \(NonEmpty ms) key -> do
      -- let keyBytes = B.pack key
      --     messages = fmap (TopicAndMessage topic . makeKeyedMessage keyBytes . B.pack) ms
      -- result <- run $ do
      --   requireAllAcks
      --   produceResps <- produceMessages messages
      --   let [[(_topicName, [(partition, NoError, offset)])]] = map _produceResponseFields produceResps

      --   resp <- fetch offset partition topic
      --   return $ fmap tamPayload . fetchMessages $ resp
      -- result `shouldBe` Right (tamPayload <$> messages)

  -- describe "withAddressHandle" $ do
    -- it "turns 'IOException's into 'KafkaClientError's" $ do
      -- result <- run $ withAddressHandle ("localhost", 9092) (\_ -> liftIO $ ioError $ userError "SOMETHING WENT WRONG!") :: IO (Either KafkaClientError ())
      -- result `shouldSatisfy` isLeft

    -- it "discards monadic effects when exceptions are thrown" $ do
      -- result <- run $ do
      --   stateName .= "expected"
      --   _ <- flip catchError (return . Left) $ withAddressHandle ("localhost", 9092) $ \_ -> do
      --     stateName .= "changed"
      --     _ <- throwError KafkaFailedToFetchMetadata
      --     n <- use stateName
      --     return (Right n)
      --   use stateName
      -- result `shouldBe` Right "expected"

  -- describe "updateMetadatas" $
    -- it "de-dupes _stateAddresses" $ do
      -- result <- run $ do
      --   stateAddresses %= NE.cons ("localhost", 9092)
      --   updateMetadatas []
      --   use stateAddresses
      -- result `shouldBe` fmap NE.nub result

{-

ProtocolType => "consumer"
 
GroupProtocol  => AssignmentStrategy
  AssignmentStrategy => String
 
MemberMetadata => Version Subscription AssignmentStrategies
  Version      => int16
  Subscription => Topics UserData
    Topics     => [String]
    UserData     => Bytes
     
MemberState => Version Assignment
  Version           => int16
  Assignment        => TopicPartitions UserData
    TopicPartitions => [Topic Partitions]
      Topic      => String
      Partitions => [int32]
    UserData     => Bytes

 * Subscription => Version Topics
 *   Version    => Int16
 *   Topics     => [String]
 *   UserData   => Bytes
 *
 * Assignment => Version TopicPartitions
 *   Version         => int16
 *   TopicPartitions => [Topic Partitions]
 *     Topic         => String
 *     Partitions    => [int32]

-}


  describe "JoinGroupRequest" $
    it "wow" $ do
      result <- run $ do
        resp <- withAddressHandle ("localhost", 9092) (flip groupCoordinator' $ GroupCoordinatorReq "milena-test-client")
        -- GroupCoordinatorResp (NoError,Broker {_brokerFields = (NodeId {_nodeId = 2},Host {_hostKString = KString {_kString = "tylers-macbook"}},Port {_portInt = 9094})})
        case resp of
          (GroupCoordinatorResp (NoError, broker)) -> do
            let rangeAssignmentProtocol = (0 :: Int16, ["milena-test" :: TopicName], "" :: KafkaBytes)
                theBytes = runPut $ serialize rangeAssignmentProtocol
                protocolMetadata = ProtocolMetadata (KBytes theBytes)
            r <- withBrokerHandle broker (\h -> joinGroup' h (JoinGroupReq ("milena-test-client", 30000, "", "consumer", [("range", protocolMetadata)])))
            liftIO $ putStrLn "\n=======" >> print r
          _ -> throwError KafkaFailedToFetchMetadata
        -- JoinGroupRequest = JoinGroupReq (ConsumerGroupId, Timeout, GroupMemberId, ProtocolType, GroupProtocols) deriving (Show, Eq, Serializable)
      result `shouldSatisfy` isRight

prop :: Testable prop => String -> prop -> SpecWith ()
prop s = it s . property
