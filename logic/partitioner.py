from logic.partitioner import get_partitions

for part in get_partitions(config):
    year = part["year"]
    month = part["month"]
    print(f"Processing: year={year}, month={month}")