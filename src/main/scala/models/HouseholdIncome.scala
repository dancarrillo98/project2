package models


/**
 * Stores data on household income for specified geographic location
 * @param mean: Mean household income
 * @param median: Median household income
 * @param dev: Standard Deviation of household income
 * @param s_w: Number of households weighted
 * @param s: Number of households
 */
class HouseholdIncome(mean: Double, median: Double, dev: Double, s_w: Double, s: Double) {

    private val hi_mean = mean
    private val hi_median: Double = median
    private val hi_stdev: Double = dev
    private val hi_sample_weight: Double = s_w
    private val hi_samples: Double = s

    def Mean(): Double = hi_mean
    def Median(): Double = hi_median
    def Dev(): Double = hi_stdev
    def Sample_Weight(): Double = hi_sample_weight
    def Samples(): Double = hi_samples
}
