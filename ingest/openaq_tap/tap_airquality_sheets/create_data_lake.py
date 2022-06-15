from datetime import datetime
import os
import pandas as pd
from glob import glob

landing_path = "./ingest/google_drive"
resource_path = "./spark_pipelines/airquality/resources"


def main():
    all_files = glob(os.path.join(landing_path, "*.ndjson"))
    ind_df = (pd.read_json(f, lines=True) for f in all_files)
    df = pd.concat(ind_df, ignore_index=True)
    file_name = "openaq-" + datetime.now().strftime("%Y%m%d%H%M%S") + ".csv"
    df.to_csv(os.path.join(resource_path, file_name), index=False)


if __name__ == '__main__':
    main()
