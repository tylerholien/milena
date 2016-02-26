{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TupleSections              #-}

module Network.Kafka.Group.Consumer.Protocol
  where
  -- () where

import Data.Int
import GHC.Exts (IsString(..))

import Network.Kafka.Group
import Network.Kafka.Protocol

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

{-

ProtocolType => "consumer"

GroupProtocol  => AssignmentStrategy
  AssignmentStrategy => String

Subscription => Version Subscription AssignmentStrategies
  Version      => int16
  Subscription => Topics UserData
    Topics     => [String]
    UserData     => Bytes

-- SyncGroupResp (KBytes {_kafkaByteString = "\NUL\NUL\NUL\NUL\NUL\SOH\NUL\vmilena-test\NUL\NUL\NUL\ACK\NUL\NUL\NUL\ACK\NUL\NUL\NUL\a\NUL\NUL\NUL\b\NUL\NUL\NUL\t\NUL\NUL\NUL\n\NUL\NUL\NUL\v\NUL\NUL\NUL\NUL"})
int8 1, int16 2, int32 4, int64, 8

correlation ID (2): \NUL\NUL\NUL\STX
error code (0): \NUL\NUL
bytes length (51): \NUL\NUL\NUL3
  \NUL\NUL\NUL\NUL\NUL\SOH\NUL\vmilena-test\NUL\NUL\NUL\ACK\NUL\NUL\NUL\ACK\NUL\NUL\NUL\a\NUL\NUL\NUL\b\NUL\NUL\NUL\t\NUL\NUL\NUL\n\NUL\NUL\NUL\v\NUL\NUL\NUL\NUL

version (0): \NUL\NUL
list length (1): \NUL\NUL\NUL\SOH
  string length (11): \NUL\v
    milena-test
    list lenth (6): \NUL\NUL\NUL\ACK
      \NUL\NUL\NUL\ACK
      \NUL\NUL\NUL\a
      \NUL\NUL\NUL\b
      \NUL\NUL\NUL\t
      \NUL\NUL\NUL\n
      \NUL\NUL\NUL\v
bytes length (0): \NUL\NUL\NUL\NUL

MemberState => Version Assignment
  Version           => int16
  Assignment        => TopicPartitions UserData
    TopicPartitions => [Topic Partitions]
      Topic      => String
      Partitions => [int32]
    UserData     => Bytes

-}
