{-# LANGUAGE OverloadedStrings #-}
module Network.Kafka where

import qualified Data.ByteString.Char8 as B
import Network
import System.IO
import Data.Serialize.Get
import Control.Monad (liftM)

import Network.Kafka.Protocol

-- fetchRequest :: RequestMessage
fr offset = FetchRequest $ FetchReq (-1, 1500, 1, [("lots", [(0, Offset offset, 1000)])])
-- FetchRequest (FetchReq (ReplicaId (-1),MaxWaitTime 15000,MinBytes 1,[(TopicName (KString "lots"),[(Partition 0,Offset 104,MaxBytes 10000)])]))

-- produceRequest :: Key -> Value -> RequestMessage
-- produceRequest k v = ProduceRequest $ ProduceReq (1, 10000, [("test", [(0, [MessageSetMember (0, (Message (1, 0, 0, k, v)))])])])

pr :: B.ByteString -> RequestMessage
pr msg = ProduceRequest (ProduceReq (RequiredAcks 1,Timeout 100,[(TopicName (KString "lots"),[(Partition 0,MessageSet [MessageSetMember (Offset 0,Message (Crc 1,MagicByte 0,Attributes 0,Key (MKB Nothing),Value (MKB (Just (KBytes msg)))))])])]))
-- Right (Response (CorrelationId 1,ProduceResponse (ProduceResp [(TopicName (KString "test"),[(Partition 0,ErrorCode 0,Offset 105)])])))

req :: RequestMessage -> IO (Either String Response)
req r = do
  let bytes = requestBytes r
  h <- connectTo "localhost" $ PortNumber 9092
  B.hPut h bytes
  hFlush h
  let reader = B.hGet h
  rawLength <- reader 4
  let (Right dataLength) = runGet (liftM fromIntegral getWord32be) rawLength
  resp <- reader dataLength
  hClose h
  return $ runGet (getResponse dataLength) resp

reqbs :: RequestMessage -> IO B.ByteString
reqbs r = do
  let bytes = requestBytes r
  h <- connectTo "localhost" $ PortNumber 9092
  B.hPut h bytes
  hFlush h
  let reader = B.hGet h
  rawLength <- reader 4
  let (Right dataLength) = runGet (liftM fromIntegral getWord32be) rawLength
  resp <- reader dataLength
  hClose h
  return resp

{-
-- CorrelationId         array len (1)         NodeId                slen (20)     str                       Port (9092)        
   "\NUL\NUL\NUL\SOH" ++ "\NUL\NUL\NUL\SOH" ++ "\NUL\NUL\NUL\NUL" ++ "\NUL\DC4" ++ "tylers-macbook.local" ++ "\NUL\NUL#\132" ++ 
-- array len (1)         TopicErrorCode (4)  slen (4)      str
   "\NUL\NUL\NUL\SOH" ++ "\NUL\NUL" ++       "\NUL\EOT" ++ "test" ++
--                                                                                                             NodeIds?
-- array len (1)         PartitionErrorCode  PartitionId           Leader                alen (1)              Replica(s)            alen (1)              Isr
   "\NUL\NUL\NUL\SOH" ++ "\NUL\NUL" ++       "\NUL\NUL\NUL\NUL" ++ "\NUL\NUL\NUL\NUL" ++ "\NUL\NUL\NUL\SOH" ++ "\NUL\NUL\NUL\NUL" ++ "\NUL\NUL\NUL\SOH" ++ "\NUL\NUL\NUL\NUL"


CorrelationId: \NUL\NUL\NUL\SOH
alen (1): \NUL\NUL\NUL\SOH
  \NUL\EOTtest
  alen (1): \NUL\NUL\NUL\SOH
    Partition: \NUL\NUL\NUL\NUL
    ErrorCode: (0): \NUL\NUL
    Offset (-1): \255\255\255\255\255\255\255\255

alen (1): \NUL\NUL\NUL\SOH
  \NUL\EOTtest
  alen (1): \NUL\NUL\NUL\SOH
    Partition: \NUL\NUL\NUL\NUL
    ErrorCode: (0): \NUL\NUL
    Offset (-1): \255\255\255\255\255\255\255\255

CorrelationId: \NUL\NUL\NUL\SOH
alen (1): \NUL\NUL\NUL\SOH
  TopicName
    slen (4): \NUL\EOT
    test
  alen (1): \NUL\NUL\NUL\SOH
    Partition: \NUL\NUL\NUL\SOH
    ErrorCode (3): \NUL\ETX
    Offset: \255\255\255\255\255\255\255\255

RequestLen (105): \NUL\NUL\NULi
ApiKey (0): \NUL\NUL
ApiVersion (0): \NUL\NUL
CorrelationId (1): \NUL\NUL\NUL\SOH
ClientId:
  slen (x): \NUL\DLEexample_producer
ProduceRequest:
  RequiredAcks (0): \NUL\NUL
  Timeout: \NUL\NUL\ENQ\220
  alen (1): \NUL\NUL\NUL\SOH
   TopicName: \NUL\EOTtest
   alen (1): \NUL\NUL\NUL\SOH
    Partition: \NUL\NUL\NUL\NUL
    MessageSetSize: \NUL\NUL\NUL3
    MessageSet:
      Offset: \NUL\NUL\NUL\NUL\NUL\NUL\NUL\NUL
      MessageSize: \NUL\NUL\NUL'
      Message:
        Crc: \229\CANvf
        MagicByte: \NUL
        Attributes: \NUL
        Key:
          blen (-1): \255\255\255\255
        Value:
          blen (x): \NUL\NUL\NUL\EM
          bytes: 2014-08-06 12:06:50 -0500

\NUL\NUL\NULP\NUL\NUL\NUL\NUL\NUL\NUL\NUL\SOH\NUL\bposeidon
ProduceRequest:
  RequiredAcks (1): \NUL\SOH
  Timeout: \NUL\NUL'\DLE
  alen (1): \NUL\NUL\NUL\SOH
    TopicName: \NUL\EOTtest
    alen (1): \NUL\NUL\NUL\SOH
      Partition: \NUL\NUL\NUL\NUL
      MessageSetSize: \NUL\NUL\NUL\SOH
      MessageSet:
        Offset: \NUL\NUL\NUL\NUL\NUL\NUL\NUL\NUL
        MessageSize: \NUL\NUL\NUL\SYN
        \DC1rE\224\NUL\NUL\NUL\NUL\NUL\EOTtest\NUL\NUL\NUL\EOTOMFG

Offset: \NUL\NUL\NUL\NUL\NUL\NUL\NUL\NUL
  Message:
    Crc: \NUL\NUL\NUL\RS
    MagicByte: \156
    Attributes: \255
    \168\ESC\NUL\NUL\NUL\NUL\NUL\ENQa key
    \NUL\NUL\NUL\vIT WORKS!!!

Message:
  Crc: \156\255\168\ESC
  MagicByte: \NUL
  Attributes: \NUL
  \NUL\NUL\NUL\ENQa key\NUL\NUL\NUL\vIT WORKS!!!

CorrelationId: \NUL\NUL\NUL\SOH
alen (2): \NUL\NUL\NUL\STX
  TopicName: \NUL\aexample
  alen (1): \NUL\NUL\NUL\SOH
    Partition: \NUL\NUL\NUL\NUL
    ErrorCode: \NUL\NUL
    Offset: \NUL\NUL\NUL\NUL\NUL\NUL\NUL\ENQ
    MessageSetSize: \NUL\NUL\NUL\213
    MessageSet:
      Offset: \NUL\NUL\NUL\NUL\NUL\NUL\NUL\NUL
      MessageSize: \NUL\NUL\NUL'
      Message:
        Crc: \169\NUL\193\ESC
        MagicByte: \NUL
        Attributes: \NUL
        Key (null): \255\255\255\255
        Value: \NUL\NUL\NUL\EM2014-08-04 16:27:04 -0500
      Offset: \NUL\NUL\NUL\NUL\NUL\NUL\NUL\SOH
      MessageSize: \NUL\NUL\NUL'
      Message:
        Crc: ~\176\234\145
        MagicByte: \NUL
        Attributes: \NUL
        Key,Val: \255\255\255\255\NUL\NUL\NUL\EM2014-08-06 12:04:29 -0500
      Offset: \NUL\NUL\NUL\NUL\NUL\NUL\NUL\STX
      MessageSize: \NUL\NUL\NUL\EM
      Message:
        Crc: \255wp,
        MagicByte,Attr: \NUL\NUL
        K,V: \NUL\NUL\NUL\aOMFG!!!\NUL\NUL\NUL\EOTOMFG
      Offset: \NUL\NUL\NUL\NUL\NUL\NUL\NUL\ETX
      MessageSize: \NUL\NUL\NUL\EM
      Message:
        Crc: \255wp,
        MB,Atr: \NUL\NUL
        K,V: \NUL\NUL\NUL\aOMFG!!!\NUL\NUL\NUL\EOTOMFG
      Offset: \NUL\NUL\NUL\NUL\NUL\NUL\NUL\EOT
      MessageSize: \NUL\NUL\NUL\EM
      Message:
        Crc: \255wp,
        MagicByte,Attr: \NUL\NUL
        K,V: \NUL\NUL\NUL\aOMFG!!!\NUL\NUL\NUL\EOTOMFG
  TopicName: \NUL\EOTtest
  alen (1): \NUL\NUL\NUL\SOH
   Partition: \NUL\NUL\NUL\NUL
   ErrorCode: \NUL\NUL
   Offset: \NUL\NUL\NUL\NUL\NUL\NUL\NUL`
   MessageSetSize: \NUL\NUL\NUL\185
   MessageSet: \NUL\NUL\NUL\NUL\NUL\NUL\NUL[\NUL\NUL\NUL\EM)nb#\NUL\NUL\255\255\255\255\NUL\NUL\NUL\vIT WORKS!!!\NUL\NUL\NUL\NUL\NUL\NUL\NUL\\\NUL\NUL\NUL\EM\255wp,\NUL\NUL\NUL\NUL\NUL\aOMFG!!!\NUL\NUL\NUL\EOTOMFG\NUL\NUL\NUL\NUL\NUL\NUL\NUL]\NUL\NUL\NUL\EM\255wp,\NUL\NUL\NUL\NUL\NUL\aOMFG!!!\NUL\NUL\NUL\EOTOMFG\NUL\NUL\NUL\NUL\NUL\NUL\NUL^\NUL\NUL\NUL\EM\255wp,\NUL\NUL\NUL\NUL\NUL\aOMFG!!!\NUL\NUL\NUL\EOTOMFG\NUL\NUL\NUL\NUL\NUL\NUL\NUL_\NUL\NUL\NUL\EM\255wp,\NUL\NUL\NUL\NUL\NUL\aOMFG!!!\NUL\NUL\NUL\EOTOMFG
-}
