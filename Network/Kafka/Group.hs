module Network.Kafka.Group
  ( groupCoordinator'
  , joinGroup'
  , syncGroup'
  , leaveGroup'
  , heartbeat'
  ) where

import System.IO
import Prelude

import Network.Kafka
import Network.Kafka.Protocol

-- * Groups
groupCoordinator' :: Handle -> GroupCoordinatorRequest -> Kafka GroupCoordinatorResponse
groupCoordinator' h request = makeRequest h $ GroupCoordinatorRR request

joinGroup' :: (Show a, Eq a, Deserializable a, Serializable a) => Handle -> JoinGroupRequest a -> Kafka (JoinGroupResponse a)
joinGroup' h request = makeRequest h $ JoinGroupRR request

syncGroup' :: (Show a, Eq a, Deserializable a, Serializable a) => Handle -> SyncGroupRequest a -> Kafka (SyncGroupResponse a)
syncGroup' h request = makeRequest h $ SyncGroupRR request

leaveGroup' :: Handle -> LeaveGroupRequest -> Kafka LeaveGroupResponse
leaveGroup' h request = makeRequest h $ LeaveGroupRR request

heartbeat' :: Handle -> HeartbeatRequest -> Kafka HeartbeatResponse
heartbeat' h request = makeRequest h $ HeartbeatRR request
