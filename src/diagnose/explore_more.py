import pandas as pd

temp = pd.read_csv("data/single_feature_ic_analysis/single_feature_rank_ic_summary_by_year.csv")
temp["abs_mean_rank_ic"] = temp["mean_rank_ic"].apply(lambda x: abs(x))
df1 = temp[temp["year"] == 2023].sort_values(by = "abs_mean_rank_ic", ascending = False)
print(df1.head(10))