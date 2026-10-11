
from pathlib import Path
import pandas as pd

DATA_PATH = (
    Path(__file__).resolve().parent.parent
    / "data"
    / "PS_20174392719_1491204439457_log.csv"
)


def main():
    if not DATA_PATH.exists():
        print(f"Dataset not found: {DATA_PATH}")
        return

    print("Loading PaySim dataset...")
    df = pd.read_csv(DATA_PATH)

    print("\n--- Dataset Overview ---")
    print(f"Rows: {len(df):,}")
    print(f"Columns: {len(df.columns)}")

    print("\n--- Column Names ---")
    print(df.columns.tolist())

    print("\n--- First 5 Records ---")
    print(df.head())

    print("\n--- Missing Values ---")
    print(df.isnull().sum())

    print("\n--- Duplicate Records ---")
    print(df.duplicated().sum())

    print("\n--- Fraud Distribution ---")
    print(df["isFraud"].value_counts(dropna=False))

    print("\n--- Transaction Types ---")
    print(df["type"].value_counts(dropna=False))

    print("\n--- Data Types ---")
    print(df.dtypes)

    print("\n--- Invalid Amounts ---")
    print("Negative amounts:", (df["amount"] < 0).sum())

    print("\n--- Invalid Fraud Labels ---")
    print((~df["isFraud"].isin([0, 1])).sum())

    print("\n--- Invalid Transaction Types ---")
    valid_types = {
        "CASH_OUT", "PAYMENT", "CASH_IN", "TRANSFER", "DEBIT"
    }
    print((~df["type"].isin(valid_types)).sum())

    print("\n--- Balance Consistency Check ---")
    expected_balance = (
        df["oldbalanceOrg"] - df["amount"]
    ).clip(lower=0)

    mismatch = (
        df["type"].isin(["TRANSFER", "CASH_OUT"])
        & (
            (df["newbalanceOrig"] - expected_balance).abs() >= 0.01
        )
    )

    print(
        "Potential origin balance mismatches:",
        mismatch.sum()
    )


if __name__ == "__main__":
    main()
