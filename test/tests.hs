{-# LANGUAGE OverloadedStrings #-}

module Main where

import Data.Functor
import Data.Either (isRight, isLeft)
import Data.List (sort)
import qualified Data.List.NonEmpty as NE
import Control.Concurrent (threadDelay)
import Control.Lens
import Control.Monad (forever, unless)
import Control.Monad.Except (catchError, throwError)
import Control.Monad.Trans (liftIO)
import Network.Kafka
import Network.Kafka.Consumer
import Network.Kafka.Group
import Network.Kafka.Group.Consumer.Protocol
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
      requireAllAcks = do
        stateRequiredAcks .= -1
        stateWaitSize .= 1
        stateWaitTime .= 1000
      byteMessages = fmap (TopicAndMessage topic . makeMessage . B.pack)

  describe "can talk to local Kafka server" $ do
    prop "can produce messages" $ \ms -> do
      result <- run . produceMessages $ byteMessages ms
      result `shouldSatisfy` isRight

    prop "can produce multiple messages" $ \(ms, ms') -> do
      result <- run $ do
        r1 <- produceMessages $ byteMessages ms
        r2 <- produceMessages $ byteMessages ms'
        return $ r1 ++ r2
      result `shouldSatisfy` isRight

    prop "can fetch messages" $ do
      result <- run $ do
        offset <- getLastOffset EarliestTime 0 topic
        withAnyHandle (\handle -> fetch' handle =<< fetchRequest offset 0 topic)
      result `shouldSatisfy` isRight

    prop "can roundtrip messages" $ \ms key -> do
      let messages = byteMessages ms
      result <- run $ do
        requireAllAcks
        info <- brokerPartitionInfo topic
        let Just PartitionAndLeader { _palLeader = leader, _palPartition = partition } = getPartitionByKey (B.pack key) info
            payload = [(TopicAndPartition topic partition, groupMessagesToSet messages)]
            s = stateBrokers . at leader
        [(_topicName, [(_, NoError, offset)])] <- _produceResponseFields <$> send leader payload
        broker <- findMetadataOrElse [topic] s (KafkaInvalidBroker leader)
        resp <- withBrokerHandle broker (\handle -> fetch' handle =<< fetchRequest offset partition topic)
        return $ fmap tamPayload . fetchMessages $ resp
      result `shouldBe` Right (tamPayload <$> messages)

    prop "can roundtrip keyed messages" $ \(NonEmpty ms) key -> do
      let keyBytes = B.pack key
          messages = fmap (TopicAndMessage topic . makeKeyedMessage keyBytes . B.pack) ms
      result <- run $ do
        requireAllAcks
        produceResps <- produceMessages messages
        let [[(_topicName, [(partition, NoError, offset)])]] = map _produceResponseFields produceResps

        resp <- fetch offset partition topic
        return $ fmap tamPayload . fetchMessages $ resp
      result `shouldBe` Right (tamPayload <$> messages)

  describe "withAddressHandle" $ do
    it "turns 'IOException's into 'KafkaClientError's" $ do
      result <- run $ withAddressHandle ("localhost", 9092) (\_ -> liftIO $ ioError $ userError "SOMETHING WENT WRONG!") :: IO (Either KafkaClientError ())
      result `shouldSatisfy` isLeft

    it "discards monadic effects when exceptions are thrown" $ do
      result <- run $ do
        stateName .= "expected"
        _ <- flip catchError (return . Left) $ withAddressHandle ("localhost", 9092) $ \_ -> do
          stateName .= "changed"
          _ <- throwError KafkaFailedToFetchMetadata
          n <- use stateName
          return (Right n)
        use stateName
      result `shouldBe` Right "expected"

  describe "updateMetadatas" $
    it "de-dupes _stateAddresses" $ do
      result <- run $ do
        stateAddresses %= NE.cons ("localhost", 9092)
        updateMetadatas []
        use stateAddresses
      result `shouldBe` fmap NE.nub result

  describe "JoinGroupRequest" $
    it "makes join requests" $ do
      result <- run $ do
        let groupId = "milena-test-client"
        resp <- withAddressHandle ("localhost", 9092) (flip groupCoordinator' $ GroupCoordinatorReq groupId)
        case resp of
          (GroupCoordinatorResp (NoError, broker)) -> do
            -- forever $ do
            --   descResp <- withBrokerHandle broker (\h -> makeRequest h $ DescribeGroupRR $ DescribeGroupReq [groupId])
            --   liftIO $ print descResp
            let rangeAssignmentProtocol = Subscription (0, [topic], UserData $ KBytes "")
                joinRequest = JoinGroupReq (groupId, 30000, "", "consumer", [GroupProtocol ("range", rangeAssignmentProtocol)])
            r <- withBrokerHandle broker (\h -> joinGroup' h joinRequest)
            liftIO $ putStrLn "\n=======" >> print r
            case r of
              LeaderJoinGroupResp genId protoName memberId members -> do
                liftIO $ print members
                -- require protocol version 0
                -- TODO: verify protocol version when deserializing and don't put it in the Members?
                tmd <- findMetadataOrElse [topic] (stateTopicMetadata . at topic) KafkaFailedToFetchMetadata
                unless (all (\ (_, Subscription (ProtocolVersion v, ts, _)) -> v == 0 && ts == [topic]) members) $
                  throwError KafkaFailedToFetchMetadata
                let partitions     = tmd ^.. partitionsMetadata . folded . partitionId
                    memberCount    = length members
                    partitionCount = length partitions
                    assignments    = map (\ (memberId', Subscription (ProtocolVersion v, ts, userData)) -> GroupAssignment (memberId', Assignment (0, [(topic, sort partitions)], userData))) members
                -- [(memberId, Subscription (ProtocolVersion 0, [topic], UserData $ KBytes ""))]
                let syncRequest = SyncGroupReq (groupId, genId, memberId, assignments) :: SyncGroupRequest (Assignment KafkaBytes)
                r' <- withBrokerHandle broker (\h -> syncGroup' h syncRequest)
                forever $
                  case r' of
                    EmptySyncGroupResp -> do
                      liftIO $ threadDelay 1000000
                      descResp <- withBrokerHandle broker (\h -> makeRequest h $ DescribeGroupRR $ DescribeGroupReq [groupId])
                      liftIO $ print descResp
                      r'' <- withBrokerHandle broker (\h -> syncGroup' h syncRequest)
                      liftIO $ print r''
                    SyncGroupResp _ -> liftIO $ print r'
                liftIO $ print r'
                return (1 :: Int)
              FollowerJoinGroupResp genId protoName leaderId memberId -> do
                liftIO $ print "hi!!!"
                let syncRequest = SyncGroupReq (groupId, genId, memberId, []) :: SyncGroupRequest (Assignment KafkaBytes)
                r' <- withBrokerHandle broker (\h -> syncGroup' h syncRequest)
                liftIO $ print r'
                case r' of
                  SyncGroupResp (Assignment (_, topicPartitions, _)) -> do
                    let offsetFetch = OffsetFetchReq (groupId, topicPartitions)
                    OffsetFetchResp tpOffsets <- withBrokerHandle broker (\h -> makeRequest h $ OffsetFetchRR offsetFetch)
                    liftIO $ print tpOffsets
                    let offsetCommit = OffsetCommitReqV2 (groupId, genId, memberId, -1, map (\ (t, ps) -> (t, map (\ (p, o, m, err) -> (p, o, m)) ps)) tpOffsets)
                    offsetCommitResponse <- withBrokerHandle broker (\h -> makeRequest h $ OffsetCommitV2RR offsetCommit)
                    liftIO $ print offsetCommitResponse
                    let heartbeat = HeartbeatReq (groupId, genId, memberId)
                    heartbeatResponse <- withBrokerHandle broker (\h -> makeRequest h $ HeartbeatRR heartbeat)
                    liftIO $ print heartbeatResponse
                    leaveGroupResponse <- withBrokerHandle broker (\h -> makeRequest h $ LeaveGroupRR $ LeaveGroupReq (groupId, memberId))
                    liftIO $ print leaveGroupResponse
                    return "oh wow..."
                  SyncGroupRespFailure e -> do
                    liftIO $ print e
                    return "holy shit!"
                return 2
              JoinGroupRespFailure err -> do
                liftIO $ print "SAD!!! :-("
                return 3
          _ -> throwError KafkaFailedToFetchMetadata
      result `shouldSatisfy` isRight

prop :: Testable prop => String -> prop -> SpecWith ()
prop s = it s . property
