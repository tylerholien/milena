{-# LANGUAGE OverloadedStrings #-}

module Main where

import Data.Functor
import Data.Either (isRight)
import Control.Monad.Trans (liftIO)
import Network.Kafka
import Network.Kafka.Producer
import Network.Kafka.Protocol (Leader(..))
import Test.Hspec
import Test.Hspec.QuickCheck
import qualified Data.ByteString.Char8 as B

main :: IO ()
main = hspec $ do
  let topic = "milena-test"
      run = runKafka ("kafka1.dev", 9093) $ defaultState "milena-test-client"
      byteMessages = fmap (TopicAndMessage topic . makeMessage . B.pack)

  describe "can talk to local Kafka server" $ do
    prop "can produce messages" $ \ms -> do
      result <- run . produceMessages $ byteMessages ms -- $ map (\m -> "omfg hi!!! length: " ++ show (length m)) (ms :: [String])
      result `shouldSatisfy` isRight

    prop "can produce multiple messages" $ \(ms, ms') -> do
      result <- run $ do
        liftIO $ putStrLn "START"
        r1 <- produceMessages $ byteMessages ms -- $ map (\m -> "omfg hi!!! length: " ++ show (length m)) (ms :: [String])
        liftIO $ putStrLn $ show r1
        r2 <- produceMessages $ byteMessages ms'
        liftIO $ putStrLn $ show r2
        liftIO $ putStrLn "DONE"
      result `shouldSatisfy` isRight

    prop "can fetch messages" $ do
      result <- run $ do
        offset <- getLastOffset EarliestTime 0 topic
        fetch =<< fetchRequest offset 0 topic
      result `shouldSatisfy` isRight

    prop "can roundtrip messages" $ \ms -> do
      let messages = byteMessages ms -- $ map (\m -> "omfg hi!!! length: " ++ show (length m)) (ms :: [String])
      result <- run $ do
        info <- brokerPartitionInfo topic
        leader <- maybe (Leader Nothing) _palLeader <$> getRandPartition info
        offset <- getLastOffset LatestTime 0 topic
        void $ send leader [(TopicAndPartition topic 0, groupMessagesToSet messages)]
        fmap tamPayload . fetchMessages <$> (fetch =<< fetchRequest offset 0 topic)
      result `shouldBe` Right (tamPayload <$> messages)


{-

import Network.Kafka
import Network.Kafka.Producer
import Network.Kafka.Protocol (Leader(..))

let topic = "milena-test"
let run = runKafka ("kafka1.dev", 9093) $ defaultState "milena-test-client"
let byteMessages = fmap (TopicAndMessage topic . makeMessage . B.pack)

let twomsgs = do { liftIO $ putStrLn "ONE"; r1 <- produceMessages $ byteMessages ["test1"]; liftIO $ putStrLn "TWO"; r2 <- produceMessages $ byteMessages ["test2"]; liftIO $ putStrLn "DONE" } :: Kafka ()

import Control.Monad (replicateM_, forM_)
:{
omg <- run $ forM_ [1..1000] $ \i -> do
  l <- liftIO $ getLine
  r1 <- produceMessages $ byteMessages ["OMG!!! MESSAGE: " ++ show i]
  liftIO $ putStrLn $ show r1
  return ()
:}

:{
run $ do
  liftIO $ putStrLn "ONE"
  r1 <- produceMessages $ byteMessages ["test1"]
  liftIO $ putStrLn "TWO"
  r2 <- produceMessages $ byteMessages ["test2"]
  liftIO $ putStrLn "DONE"
:}

-}
