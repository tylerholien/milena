{ mkDerivation, base, bytestring, cereal, containers, digest
, either, hspec, lens, mtl, network, QuickCheck, random
, resource-pool, stdenv, transformers
}:
mkDerivation {
  pname = "milena";
  version = "0.2.0.0";
  src = ./.;
  buildDepends = [
    base bytestring cereal containers digest either lens mtl network
    random resource-pool transformers
  ];
  testDepends = [ base bytestring hspec network QuickCheck ];
  description = "A Kafka client for Haskell";
  license = stdenv.lib.licenses.bsd3;
}
