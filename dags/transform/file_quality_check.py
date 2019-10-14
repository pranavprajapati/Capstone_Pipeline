	# 1. CHECK FOR MISSING VALUES AND FILL
	nans = lambda df: df[df.isna().any(axis=1)]
	nandf = len(nans(df))

	if (nandf > 0):
		print("MISSING VALUES DETECTED")
		df = fillEmpty(df)
		print("MISSING VALUES FILLED")
	else:
		print('NO MISSING VALUES')

# 2. CHECK FOR DUPLICATE ROWS AND REMOVE
	dup_no = len(df[df.duplicated()])
	if (dup_no > 0):
		print( f"{file} HAS {dup_no} duplicates.")
		df = df.drop_duplicates()
	else:
		print(f"{file} HAS NO duplicates.")
