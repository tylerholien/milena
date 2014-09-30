module Main where

-- import Network.Kafka
import Test.Hspec

main :: IO ()
main = hspec $ do

  describe "can talk to local Kafka server" $ do
    it "roundtrips producer -> consumer" $ do
      putStrLn "hello"
      1 `shouldBe` 0
