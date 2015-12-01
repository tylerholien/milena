{-# LANGUAGE OverloadedStrings #-}

module Main where

import Data.Functor
import Data.Either (isRight, isLeft)
import qualified Data.List.NonEmpty as NE
import Control.Lens
import Control.Monad.Except (catchError, throwError)
import Control.Monad.Trans (liftIO)
import Network.Kafka
import Network.Kafka.Producer
import Network.Kafka.Protocol (Leader(..))
import Test.Tasty
import Test.Tasty.Hspec
import Test.Tasty.QuickCheck
import qualified Data.ByteString.Char8 as B

import Prelude

main :: IO ()
main = testSpec "the specs" specs >>= defaultMain

specs :: Spec
specs = do
  let topic = "milena-test"
      run = runKafka $ mkKafkaState "milena-test-client" ("localhost", 9092)
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

    prop "can roundtrip messages" $ \ms -> do
      let messages = byteMessages ms
      result <- run $ do
        info <- brokerPartitionInfo topic
        leader <- maybe (Leader Nothing) _palLeader <$> getRandPartition info
        offset <- getLastOffset LatestTime 0 topic
        void $ send leader [(TopicAndPartition topic 0, groupMessagesToSet messages)]
        fmap tamPayload . fetchMessages <$> withAnyHandle (\handle -> fetch' handle =<< fetchRequest offset 0 topic)
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

prop :: Testable prop => String -> prop -> SpecWith ()
prop s = it s . property
