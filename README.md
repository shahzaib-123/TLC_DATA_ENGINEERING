
# Data Engineering Tech Task

### Project Objectives

1. **Merge Input Files**: The input data is spread over several files, including separate files for “Yellow” and “Green” taxis. Merging these files makes sense for unified analysis and reduces redundancy. However, for practical reasons and the scope of this task, I didnt merge them.

2. **Hour-level and Day of Week-level**: The input data contains “date and time” columns. To evaluate data on an hour-level and day of week-level, additional columns were created during data processing. This affects the output structure as it includes aggregated metrics by hour and day of the week.

3. **Key Metrics**:
   - **Average Distance Driven by Yellow and Green Taxis Per Hour**
   - **Day of the Week in 2019 and 2020 with the Lowest Number of Single Rider Trips**
   - **Top 3 Busiest Hours**

### Results

#### Average Distance Driven by Yellow and Green Taxis Per Hour

| pickup_hour | average_distance |
|-------------|------------------|
| 00          | 0.400000         |
| 01          | 1.300000         |
| 07          | 0.100000         |
| 09          | 2.200000         |
| 11          | 3.100000         |
| 13          | 18.300000        |
| 14          | 10.600000        |
| 15          | 1.000000         |
| 16          | 0.000000         |
| 17          | 1.050000         |
| 19          | 8.633333         |
| 20          | 1.200000         |
| 21          | 1.750000         |
| 23          | 1.050000         |

#### Day of the Week in 2019 and 2020 with the Lowest Number of Single Rider Trips

| pickup_day_of_week | single_rider_trips |
|--------------------|--------------------|
| 2                  | 1                  |

#### Top 3 Busiest Hours

| pickup_hour | number_of_trips |
|-------------|-----------------|
| 19          | 3               |
| 23          | 2               |
| 21          | 2               |

### Usage

#### Dependencies

- Python 3.x
- pandas
- requests
- pyarrow
- fastavro
- sqlite3

Install dependencies using:

```bash
pip install -r requirements.txt
```

#### Running the Script

Execute the main script to fetch and process the data:

```bash
python pipeline.py
```

### Cron Job for Automation

To automate this pipeline such that it pulls new data each month, set up a cron job on a Unix-like system:

1. Open the crontab file:

```bash
crontab -e
```

2. Add the following line to schedule the script to run at the start of every month:

```bash
0 0 1 * * /usr/bin/python3 /path/to/pipeline.py
```

This cron job will run the `pipeline.py` script at midnight on the first day of each month.

### Limitations and AWS Consideration

To optimize the pipeline, we could have used S3 bucket & AWS tools. However, since they are paid services and I did not have access to them, I used Python and SQLite on my local system. Storing and processing large files locally was challenging, so I limited the scope to yellow and green taxi data for 2019 and 2020. Despite these limitations, the project successfully answers the specified questions.

---

