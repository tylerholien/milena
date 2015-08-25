{-# LANGUAGE OverloadedStrings #-}

module Main where

import Data.Functor
import Data.Either (isRight)
import Network.Kafka
import Network.Kafka.Producer
import Network.Kafka.Protocol (Leader(..))
import Test.Hspec
import Test.Hspec.QuickCheck
import qualified Data.ByteString.Char8 as B

main :: IO ()
main = hspec $ do
  let topic = "milena-test"
      run = runKafka ("localhost", 9092) $ defaultState "milena-test-client"
      byteMessages = fmap (TopicAndMessage topic . makeMessage . B.pack)

  describe "can talk to local Kafka server" $ do
    prop "can produce messages" $ \ms -> do
      result <- run . produceMessages $ byteMessages ms
      result `shouldSatisfy` isRight

    prop "can fetch messages" $ do
      result <- run $ do
        offset <- getLastOffset EarliestTime 0 topic
        fetch =<< fetchRequest offset 0 topic
      result `shouldSatisfy` isRight

    prop "can roundtrip messages" $ \ms -> do
      let messages = byteMessages ms
      result <- run $ do
        info <- brokerPartitionInfo topic
        leader <- maybe (Leader Nothing) _palLeader <$> getRandPartition info
        offset <- getLastOffset LatestTime 0 topic
        void $ send leader [(TopicAndPartition topic 0, groupMessagesToSet messages)]
        fmap tamPayload . fetchMessages <$> (fetch =<< fetchRequest offset 0 topic)
      result `shouldBe` Right (tamPayload <$> messages)
