import pandas as pd
import pandera as pa
import fire


def validate_data(datafile):
	# Define the data validation schema
	start_year = 2010
	end_year = 2025
	schema = pa.DataFrameSchema(
		columns={
			"province": pa.Column(pa.String, checks=pa.Check.str_matches(r"^[ก-๙]+$"), nullable=False),
			"ratio_tourist_stay": pa.Column(pa.Float, checks=pa.Check.ge(0), nullable=False),
			"no_tourist_stay": pa.Column(pa.Int, checks=pa.Check.ge(0), nullable=False),
			"no_tourist_all": pa.Column(pa.Int, checks=pa.Check.ge(0), nullable=False),
			"no_tourist_thai": pa.Column(pa.Int, checks=pa.Check.ge(0), nullable=False),
			"no_tourist_foreign": pa.Column(pa.Int, checks=pa.Check.ge(0), nullable=False),
			"profit_all": pa.Column(pa.Float, nullable=False),
			"profit_tourist_thai": pa.Column(pa.Float, nullable=False),
			"profit_tourist_foreign": pa.Column(pa.Float, nullable=False),
			"year": pa.Column(pa.Int8, checks=pa.Check.between(start_year - 1, end_year), nullable=False),
			"month": pa.Column(pa.String, checks=pa.Check.str_matches(r"^[ก-๙.]+$"), nullable=False),
			"month_no": pa.Column(pa.Int8, checks=pa.Check.between(1, 12), nullable=False),
		},
		checks=[
			pa.Check(lambda df: df["profit_all"] == df["profit_tourist_thai"] + df["profit_tourist_foreign"]),
			pa.Check(lambda df: df["no_tourist_all"] == df["no_tourist_thai"] + df["no_tourist_foreign"]),
		],
	)

	# Load the data into a pandas DataFrame object
	combined_df = pd.read_csv(datafile)

	# Validate the data against the schema
	validated_df = schema.validate(combined_df)

	# Print the first 5 rows of the validated DataFrame
	print(validated_df.head())


if __name__ == "__main__":
	fire.Fire(validate_data)
