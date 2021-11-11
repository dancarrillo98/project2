package models

class Age(m_avg: Double, m_med: Double, m_dev: Double, m_s_w: Double, m_s: Double,
          f_avg: Double, f_med: Double, f_dev: Double, f_s_w: Double, f_s: Double) {
    private val male_mean: Double = m_avg
    private val male_median: Double = m_med
    private val male_dev: Double = m_dev
    private val male_sample_weight: Double = m_s_w
    private val male_samples: Double = m_s
    private val female_mean: Double = f_avg
    private val female_median: Double = f_med
    private val female_dev: Double = f_dev
    private val female_sample_weight: Double = f_s_w
    private val female_samples: Double = f_s

    def Male_Mean(): Double = male_mean
    def Male_Median() : Double = male_median
    def Male_Dev(): Double = male_dev
    def Male_Sample_Weight(): Double = male_sample_weight
    def Male_Samples(): Double = male_samples
    def Female_Mean(): Double = female_mean
    def Female_Median(): Double = female_median
    def Female_Dev(): Double = female_dev
    def Female_Sample_Weight(): Double = female_sample_weight
    def Female_Samples(): Double = female_samples
}
