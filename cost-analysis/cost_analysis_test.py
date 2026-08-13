import unittest
from dataclasses import replace

from cost_analysis import DEFAULTS, compute


class CostAnalysisTest(unittest.TestCase):
  def test_strong_metadata_reads_double_only_metadata_scan_capacity(self) -> None:
    eventual = compute(replace(DEFAULTS, metadata_strong_reads=False))
    strong = compute(replace(DEFAULTS, metadata_strong_reads=True))

    expected_increase = (
      eventual["req_ddb_r_scan_fast"] + eventual["req_ddb_r_scan_recovery"]
    )
    self.assertEqual(strong["rru_hour"] - eventual["rru_hour"], expected_increase)
    self.assertEqual(strong["wru_hour"], eventual["wru_hour"])

  def test_transactional_metadata_writes_add_fence_reads_and_double_metadata_writes(self) -> None:
    plain = compute(replace(DEFAULTS, transactional_metadata_writes=False))
    transactional = compute(replace(DEFAULTS, transactional_metadata_writes=True))

    self.assertEqual(
      transactional["segment_wru_hour"] - plain["segment_wru_hour"],
      plain["s_seg_per_hour"],
    )
    self.assertEqual(
      transactional["transactional_fence_rru_hour"],
      plain["s_seg_per_hour"] * DEFAULTS.avg_partitions_per_segment * 2.0,
    )


if __name__ == "__main__":
  unittest.main()
