{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}

module Network.Kafka.Group where

import System.IO
import Prelude

import Network.Kafka
import Network.Kafka.Protocol

-- * Groups
-- | Execute a fetch request and get the raw fetch response.
groupCoordinator' :: Handle -> GroupCoordinatorRequest -> Kafka GroupCoordinatorResponse
groupCoordinator' h request =
    makeRequest (GroupCoordinatorRequest request) >>= doRequest h >>= expectResponse ExpectedGroupCoordinator _GroupCoordinatorResponse

joinGroup' :: (Show a, Eq a, Serializable a) => Handle -> JoinGroupRequest a -> Kafka JoinGroupResponse
joinGroup' h request =
  makeRequest (JoinGroupRequest request) >>= doRequest h >>= expectResponse ExpectedJoinGroup _JoinGroupResponse


-- let rangeAssignmentProtocol = (0 :: Int16, ["milena-test" :: TopicName], "" :: KafkaBytes)
--     theBytes = runPut $ serialize rangeAssignmentProtocol
--     protocolMetadata = ProtocolMetadata (KBytes theBytes)
