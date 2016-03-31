{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TupleSections              #-}

module Network.Kafka.Group.Consumer.Protocol
  ( protocolType
  , assignmentV0
  , subscriptionV0
  , AssignmentStrategy(..)
  , Subscription(..)
  , UserData(..)
  , ProtocolVersion(..)
  , Assignment(..)
  ) where

import           Data.Int
import           GHC.Exts               (IsString (..))

import           Network.Kafka.Protocol

protocolType :: ProtocolType
protocolType = "consumer"

data AssignmentStrategy = Range
                        | RoundRobin
                        deriving (Show, Eq)

instance Serializable AssignmentStrategy where
  serialize Range      = serialize ("range" :: KafkaString)
  serialize RoundRobin = serialize ("roundrobin" :: KafkaString)

instance Deserializable AssignmentStrategy where
  deserialize = do
    s <- deserialize
    case (s :: KafkaString) of
      "range"      -> return Range
      "roundrobin" -> return RoundRobin
      _            -> fail $ "Unexpected assignment strategy: " ++ show s

newtype Subscription a = Subscription (ProtocolVersion, [TopicName], UserData a) deriving (Show, Eq, Deserializable, Serializable)
newtype UserData a = UserData a deriving (Show, Eq, Deserializable, Serializable)
newtype ProtocolVersion = ProtocolVersion Int16 deriving (Show, Eq, Num, Deserializable, Serializable)

subscriptionV0 :: [TopicName] -> UserData a -> Subscription a
subscriptionV0 = Subscription .: (0,,)

newtype Assignment a = Assignment (ProtocolVersion, [(TopicName, [Partition])], UserData a) deriving (Show, Eq, Deserializable, Serializable)

assignmentV0 :: [(TopicName, [Partition])] -> UserData a -> Assignment a
assignmentV0 = Assignment .: (0,,)

infixr 9 .:
(.:) :: (c -> d) -> (a -> b -> c) -> a -> b -> d
(.:) = (.).(.)
