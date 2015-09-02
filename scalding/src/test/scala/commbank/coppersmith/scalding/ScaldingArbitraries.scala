package commbank.coppersmith.scalding

import org.scalacheck.Arbitrary

import org.apache.hadoop.fs.Path

import commbank.coppersmith.Arbitraries.arbNonEmptyAlphaStr

object ScaldingArbitraries {
  implicit val arbPath: Arbitrary[Path] = Arbitrary(arbNonEmptyAlphaStr.map(nes => new Path(nes.value)))
}
