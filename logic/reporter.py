

import csv
import os


def write_discrepancies_to_csv(discrepancies: list[dict], output_path: str):
    """
    Writes a list of discrepancy records to a CSV file.

    Each record in the list should be a dictionary with consistent keys.
    """
    if not discrepancies:
        print("No discrepancies found.")
        return

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=discrepancies[0].keys())
        writer.writeheader()
        writer.writerows(discrepancies)

    print(f"Discrepancy report written to: {output_path}")