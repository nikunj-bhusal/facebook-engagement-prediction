import pandas as pd

INPUT_FILE = "candidates_list.csv"

df = pd.read_csv(INPUT_FILE)

combos = (
    df[["EnglishDistrictName", "ConstName"]]
    .drop_duplicates()
    .sort_values(["EnglishDistrictName", "ConstName"])
    .reset_index(drop=True)
)
counts = df.groupby(["EnglishDistrictName", "ConstName"]).size()

selected = []

print(f"\n{len(combos)} constituencies — press Enter to keep (default y), n to skip\n")

for _, row in combos.iterrows():
    d, c = row["EnglishDistrictName"], row["ConstName"]
    n = counts[(d, c)]
    ans = input(f"  {d.title()} - {c}  ({n} candidates) [Y/n]: ").strip().lower()
    if ans != "n":
        selected.append((d, c))

if not selected:
    print("\nNothing selected. Exiting.")
    exit()

mask = df.apply(
    lambda r: (r["EnglishDistrictName"], r["ConstName"]) in selected, axis=1
)
selected_df = df[mask].reset_index(drop=True)

print(f"\n✅ {len(selected)} constituencies, {len(selected_df)} candidates selected:")
for d, c in selected:
    print(f"   • {d.title()} - {c}")

selected_df.to_csv("selected_candidates.csv", index=False)
print(f"\n💾 Saved to selected_candidates.csv")
