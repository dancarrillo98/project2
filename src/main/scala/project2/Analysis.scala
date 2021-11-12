package project2

// Class for creating table from relevant columns
case class Table(question: String, columns: Seq[String], viewName: String, sqlStatement: String, filename: String)

// Class for calculating coefficient based on Table
case class Coefficient(viewName: String, sqlStatement: String, filename: String)

/**
  * Each question will need to create Table and Coefficient objects using the schema above
  * 
  */
object Analysis {


    // Question 5: "Do high school degrees correlate with less debt?"
    val q5Table = new Table(
        "Do high school degrees correlate with less debt?",
        Seq("city", "state", "debt", "hs_degree"),
        "Debt_HSDegree",
        "SELECT state AS State, ROUND(AVG(CAST(debt AS decimal)*100), 2) AS Debt_Percentage, ROUND(AVG(CAST(hs_degree AS decimal)*100), 2) AS High_School_Degree_Percentage FROM Debt_HSDegree GROUP BY State ORDER BY Debt_Percentage DESC",
        "q5_Table"
    )

    val q5Coef = new Coefficient(
        "Debt_HSDegree_Adjusted",
        "SELECT ROUND(((AVG(Debt_Percentage*High_School_Degree_Percentage)-(AVG(Debt_Percentage)*AVG(High_School_Degree_Percentage))) / (STD(Debt_Percentage)*STD(High_School_Degree_Percentage))), 2) AS q5_coefficient FROM Debt_HSDegree_Adjusted",
        "q5_Coef"
    )


    // Question 6: "Does general population age correlate with less debt?"
    val q6Table = new Table(
        "Does general population age correlate with less debt?",
        Seq("city", "state", "male_age_mean", "female_age_mean", "debt"),
        "Age_Debt",
        "SELECT state AS State, ROUND(AVG(CAST(debt AS decimal)*100), 2) AS Debt_Percentage, ROUND(AVG((CAST(male_age_mean AS decimal) + CAST(female_age_mean as decimal))/2), 2) AS Average_Age FROM Age_Debt GROUP BY State ORDER BY Debt_Percentage DESC",
        "q6_Table"
    )

    val q6Coef = new Coefficient(
        "Age_Debt_Adjusted",
        "SELECT ROUND(((AVG(Debt_Percentage*Average_Age)-(AVG(Debt_Percentage)*AVG(Average_Age))) / (STD(Debt_Percentage)*STD(Average_Age))), 2) AS q6_coefficient FROM Age_Debt_Adjusted",
        "q6_Coef"
    )
   

    // Question 10: "Does a larger population mean higher rent?"
    val q10Table = new Table(
        "Does a larger population mean higher rent?",
        Seq("city", "state", "pop", "rent_mean"),
        "Rent_Pop",
        "SELECT city AS City, state AS State, SUM(CAST(pop AS decimal)) AS Population, ROUND(AVG(CAST(rent_mean AS decimal)), 2) AS AverageRent FROM Rent_Pop GROUP BY city, state ORDER BY Population DESC",
        "q10_Table"
    )

    val q10Coef = new Coefficient(
        "Rent_Pop_Adjusted",
        "SELECT ROUND(((AVG(Population*AverageRent)-(AVG(Population)*AVG(AverageRent))) / (STD(Population)*STD(AverageRent))), 2) AS q10_coefficient FROM Rent_Pop_Adjusted",
        "q10_Coef"
    )


} // end class