package com.ebay.app.raptor.chocolate.common;

import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Misc math utilities because I suck at OO
 * 
 * @author jepounds
 */
public class MathUtilities {
    /**
     * Given a number, returns a partitioned version of them according to a particular distribution. For example, if:
     * range_end = 21 num_partitions = 7 partition_index = 5 This means we are trying to divide this range into 7 even
     * partitions: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20] The 7 partitions are
     * necessarily sequential, so: 0=[0, 1, 2], 1=[3, 4, 5], 2=[6, 7, 8], 3=[9, 10, 11], 4=[12, 13, 14], 5=[15, 16, 17],
     * 6=[18, 19, 20] And partition_index is 0-based, so 5 would represent this partition=[15, 16, 17] And thus we
     * return the pair representation of [15, 18] because the end of the partition range is non-inclusive. REPEAT, THE
     * END IS NON-INCLUSIVE
     * 
     * @param total_range_end we will assume we are dividing between [0..range_end)
     * @param num_partitions this is the number of partitions to use. must be 1 < num_partitions <= total_range_end
     * @param partition_index this is the index of partition to fetch. must be 0 <= index < num_partitions
     */
    public static Pair<Integer, Integer> getPartitionRange(int total_range_end, int num_partitions,
            int partition_index) {
        Validate.isTrue(total_range_end >= 1);
        Validate.isTrue(num_partitions >= 1 && num_partitions <= total_range_end);
        Validate.isTrue(partition_index >= 0 && partition_index < num_partitions);

        // Key variables here.
        int num_per_partition = (total_range_end / num_partitions);
        int leftover = (total_range_end % num_partitions);

        // Get the start index to begin with.
        // We have to pad the start index to account for offsets
        int start_index = partition_index * num_per_partition + Math.min(leftover, partition_index);

        // Now there will be some left over items. We will distribute them at the beginning
        // of the range.
        int end_index = start_index + num_per_partition + ((partition_index < leftover) ? 1 : 0);

        // ta-ta
        return Pair.of(start_index, end_index);
    }

    /**
     * The reverse of the getPartitionRange function. Returns the partition index given a total range, a number of
     * partitions, and an identifier.
     * 
     * @param total_range_end The total range end. typically 997
     * @param num_partitions The number of partitions all in all.
     * @param pre_mod the identifier we've done a modulus on. must be less than total_range_end
     * @return the index of the partition we're using. 
     */
    public static int getPartitionIndex(int total_range_end, int num_partitions, int pre_mod) {
        Validate.isTrue(pre_mod < total_range_end && pre_mod >= 0, "pre_mod must be within modulo range");
        final int unit = total_range_end / num_partitions; // cut the whole 997 range into units of this size
        final int remainder = total_range_end - unit * num_partitions; // these are leftovers
        final int unit_plus = unit + 1; // this is the plus-size leading units to "absorb" the leftover one per each
                                        // unit
        final int parting_index = remainder * unit_plus; // this is the parting index before which units are plus-size,
                                                         // after which, are normal size

        if (pre_mod < parting_index) {
            return pre_mod / unit_plus;
        } else {
            return remainder + (pre_mod - parting_index) / unit;
        }
    }
}
