import pandas as pd

# Load the CSV file with the first row as the header
file_path = 'KurumListe.csv'  # Update to the correct file path if needed
df = pd.read_csv(file_path, header=1)

# Print the column names to check for discrepancies
print(df.columns.tolist())
