package de.lwerner.flink.percentiles.math;

import java.util.List;

/**
 * QuickSelect is a QuickSort like algorithm to select the kth smallest element in a set of values.
 *
 * @author Lukas Werner
 */
public class QuickSelect {

    /**
     * Entry point for this algorithm
     *
     * @param list the list of values
     * @param k the rank
     *
     * @return the actual value
     */
    public float select(List<Float> list, int k) {
        return select(list, k, false);
    }

    /**
     * Entry point for this algorithm
     *
     * @param list the list of values
     * @param k the rank
     * @param recursive should this algorithm run recursively?
     *
     * @return the actual value
     */
    public float select(List<Float> list, int k, boolean recursive) {
        if (recursive) {
            return selectRecursive(list, 0, list.size() - 1, k);
        }

        return select(list, list.size() - 1, k);
    }

    /**
     * Quicksort like selection algorithm (recursive variant).
     *
     * @param list the values list
     * @param left the left bound
     * @param right the right bound
     * @param k the rank
     *
     * @return the result
     */
    private float selectRecursive(List<Float> list, int left, int right, int k) {
        if (left == right) {
            return list.get(left);
        }

        int pivotIndex = left + (int) Math.floor((right - left + 1) / 2.0);
        pivotIndex = partition(list, left, right, pivotIndex);

        if (k == pivotIndex) {
            return list.get(k);
        } else if (k < pivotIndex) {
            return selectRecursive(list, left, pivotIndex - 1, k);
        } else {
            return selectRecursive(list, pivotIndex + 1, right, k);
        }
    }

    /**
     * Quicksort like selection algorithm.
     *
     * @param list the values list
     * @param right the right bound
     * @param k the rank
     *
     * @return the result
     */
    private float select(List<Float> list, int right, int k) {
        int left = 0;
        while (true) {
            if (left == right) {
                return list.get(left);
            }

            int pivotIndex = left + (int) Math.floor((right - left + 1) / 2.0);
            pivotIndex = partition(list, left, right, pivotIndex);

            if (k == pivotIndex) {
                return list.get(k);
            } else if (k < pivotIndex) {
                right = pivotIndex - 1;
            } else {
                left = pivotIndex + 1;
            }
        }
    }

    /**
     * Method to partition the list
     *
     * @param list the values list
     * @param left the left bound
     * @param right the right bound
     * @param pivotIndex the current pivot index
     *
     * @return the new pivot index
     */
    private int partition(List<Float> list, int left, int right, int pivotIndex) {
        float pivotValue = list.get(pivotIndex);
        swap(list, pivotIndex, right);
        int storeIndex = left;

        for (int i = left; i < right; i++) {
            if (list.get(i) < pivotValue) {
                swap(list, storeIndex, i);
                storeIndex++;
            }
        }

        swap(list, right, storeIndex);

        return storeIndex;
    }

    /**
     * Swap method to exchange to values in list by their indexes
     *
     * @param list the values list
     * @param index1 the first index
     * @param index2 the second index
     */
    private void swap(List<Float> list, int index1, int index2) {
        float temp = list.get(index1);
        list.set(index1, list.get(index2));
        list.set(index2, temp);
    }

}