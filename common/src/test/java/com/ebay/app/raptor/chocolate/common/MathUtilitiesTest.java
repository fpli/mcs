package com.ebay.app.raptor.chocolate.common;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("javadoc")
public class MathUtilitiesTest {

    @Test(expected = IllegalArgumentException.class)
    public void testPartitionRangeInvalidTotal() {
        MathUtilities.getPartitionRange(0, 1, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPartitionRangeInvalidNumPartitions() {
        MathUtilities.getPartitionRange(5, 0, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPartitionRangeMaxNumPartitions() {
        MathUtilities.getPartitionRange(5, 6, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPartitionRangeInvalidIndex() {
        MathUtilities.getPartitionRange(5, 3, -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPartitionRangeInvalidIndex2() {
        MathUtilities.getPartitionRange(5, 3, 3);
    }

    // Adapted from Lian's code
    private int getPartition(int prePartitionModulus, int identifier, int numPartitions) {
        final int unit = prePartitionModulus / numPartitions; // cut the whole 997 range into units of this size
        final int remainder = prePartitionModulus - unit * numPartitions; // these are leftovers
        final int unitPlus = unit + 1; // this is the plus-size leading units to "absorb" the leftover one per each unit
        final int partingIdx = remainder * unitPlus; // this is the parting index before which units are plus-size,
                                                     // after which, are normal size

        int preMod = identifier % 997;
        if (preMod < partingIdx) {
            return preMod / unitPlus;
        } else {
            return remainder + (preMod - partingIdx) / unit;
        }
    }

    private void testRange(int total_range_end) {
        for (int num_partitions = 1; num_partitions <= total_range_end; ++num_partitions) {
            int prev_first = 0;
            int sum = 0;
            // System.out.print("[0," + total_range_end + "), partitions=" + num_partitions + "; ");
            for (int partition_index = 0; partition_index < num_partitions; ++partition_index) {
                Pair<Integer, Integer> range = MathUtilities.getPartitionRange(total_range_end, num_partitions,
                        partition_index);
                assertEquals(prev_first, range.getLeft().intValue());
                prev_first = range.getRight();
                int diff = range.getRight() - range.getLeft();
                int partition_size = total_range_end / num_partitions;
                sum += diff;
                // System.out.print("index=" + partition_index + " val=" + range + "; ");
                assertTrue(diff == partition_size || diff == (partition_size + 1));
                for (int in_range = range.getLeft(); in_range < range.getRight(); ++in_range) {
                    assertEquals(partition_index, getPartition(total_range_end, in_range, num_partitions));
                    assertEquals(partition_index,
                            MathUtilities.getPartitionIndex(total_range_end, num_partitions, in_range % 997));
                }
            }
            assertEquals(total_range_end, sum);
            // System.out.println("");
        }

    }

    @Test
    public void testPartitionRange() {
        // Degenerate test
        {
            Pair<Integer, Integer> range = MathUtilities.getPartitionRange(1, 1, 0);
            assertEquals(0, range.getLeft().intValue());
            assertEquals(1, range.getRight().intValue());
        }

        // Simple test spanning one partition
        {
            Pair<Integer, Integer> range = MathUtilities.getPartitionRange(3, 1, 0);
            assertEquals(0, range.getLeft().intValue());
            assertEquals(3, range.getRight().intValue());
        }

        // Simple test spanning 2 partitions
        {
            Pair<Integer, Integer> range1 = MathUtilities.getPartitionRange(3, 2, 0);
            Pair<Integer, Integer> range2 = MathUtilities.getPartitionRange(3, 2, 1);
            assertEquals(0, range1.getLeft().intValue());
            assertEquals(2, range1.getRight().intValue());
            assertEquals(2, range2.getLeft().intValue());
            assertEquals(3, range2.getRight().intValue());
        }
        for (int i = 1; i <= 25; ++i)
            testRange(i);
        testRange(997);
    }

}
