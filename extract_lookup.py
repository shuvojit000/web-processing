import pandas as pd

lookupdf = pd.read_csv("po-processing/cameron-lookup-table.csv", encoding="ISO-8859-1", header=None)

print(lookupdf.head())

lookup_dict = dict(zip(lookupdf[7], lookupdf[8]))
print(lookup_dict)
