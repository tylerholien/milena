{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}

module Network.Kafka.Group
  ( groupCoordinator'
  , joinGroup'
  , syncGroup'
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

-- let rangeAssignmentProtocol = (0 :: Int16, ["milena-test" :: TopicName], "" :: KafkaBytes)
--     theBytes = runPut $ serialize rangeAssignmentProtocol
--     protocolMetadata = ProtocolMetadata (KBytes theBytes)

{-

data GroupFSM' where
  Down :: Startup -> '[ DiscoveringCoordinator ]

  DiscoveringCoordinator :: GroupCoordinatorRequest -> '[ GroupCoordinatorResponseFailure -> DiscoveringCoordinator
                                                        , GroupCoordinatorResponseSuccess -> JoiningGroup
                                                        ]

  JoiningGroup :: JoinGroupRequest -> '[ JoinGroupRespFailure -> DiscoveringCoordinator
                                       , LeaderJoinGroupResp -> Leading
                                       , FollowerJoinGroupResp -> AwaitingAssignment
                                       ]

  Leading :: ValidateMetadata -> '[ MismatchedMetadata -> ForwardingAssignmentFailureToBroker
                                  , ValidMetadata -> Assigning
                                  ]

  ForwardingAssignmentFailureToBroker :: LeaveGroup -> '[ LeaveGroupResp -> JoiningGroup ]

  Assigning :: PropagateState -> '[ StatePropagated -> GroupMember
                                  , CoordinatorFailed -> DiscoveringCoordinator
                                  , Rebalance -> JoiningGroup
                                  ]

  GroupMember :: Heartbeat -> '[ HeartbeatResp -> GroupMember
                               , CoordinatorFailed -> DiscoveringCoordinator
                               , Rebalance -> JoiningGroup
                               ]

  AwaitingAssignment :: SyncGroupRequest -> '[ Assigned -> GroupMember
                                             , CoordinatorFailed -> DiscoveringCoordinator
                                             , Rebalance -> JoiningGroup
                                             ]

-}

data GroupFSM = Down
              | DiscoveringCoordinator
              | Assigning
              | AwaitingAssignment
              | GroupLeader
              | GroupFollower
              | CoordinatorRediscovery
              | Stopped

{-

Down
  --- begin ---> CoordinatorDiscovery

CoordinatorDiscovery
  --- empty join ---> 

GroupLeader

CoordinatorRediscovery

Stopped

-}
