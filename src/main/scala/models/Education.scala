package models

class Education(t: Double, m: Double, f: Double) {
    private val hs_degree: Double = t
    private val hs_degree_male: Double = m
    private val hs_degree_female: Double = f

    def HS_Degree: Double = hs_degree
    def HS_Degree_Male: Double = hs_degree_male
    def HS_Degree_Female: Double = hs_degree_female
}
