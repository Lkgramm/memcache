import argparse
import csv
import gzip
import glob
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List

import memcache
from memc_load import appsinstalled_pb2 as pb

NORMAL_ERR_RATE = 0.01
BATCH_SIZE = 100

devtype_to_memc = {
    "idfa": None,
    "gaid": None,
    "adid": None,
    "dvid": None,
}


def insert_to_memc(
    dev_type: str,
    dev_id: str,
    lat: float,
    lon: float,
    apps: List[int],
    dry: bool = False,
) -> bool:
    if not apps:
        return True

    ua = pb.UserApps()
    ua.lat = lat
    ua.lon = lon
    ua.apps.extend(apps)
    key = f"{dev_type}:{dev_id}"
    packed = ua.SerializeToString()

    if dry:
        logging.debug(f"[DRY] {dev_type}:{dev_id} -> {ua}")
        return True

    try:
        res = devtype_to_memc[dev_type].set(key, packed)
        if not res:
            logging.warning(f"Failed to insert: {key}")
        return res
    except Exception as e:
        logging.exception(f"Exception on insert: {key}: {e}")
        return False


def process_file(filepath: str, dry: bool = False, workers: int = 10):
    logging.info(f"Processing {filepath}")
    total, errors = 0, 0
    open_func = gzip.open if filepath.endswith(".gz") else open
    with open_func(filepath, mode="rt", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="\t")
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = []
            for row in reader:
                if len(row) != 5:
                    errors += 1
                    continue
                dev_type, dev_id, lat, lon, apps = row
                if dev_type not in devtype_to_memc:
                    errors += 1
                    continue
                try:
                    lat, lon = float(lat), float(lon)
                    apps = list(map(int, apps.split(","))) if apps else []
                except ValueError:
                    errors += 1
                    continue

                future = executor.submit(
                    insert_to_memc, dev_type, dev_id, lat, lon, apps, dry
                )
                futures.append(future)
                total += 1

            for future in as_completed(futures):
                if not future.result():
                    errors += 1

    err_rate = errors / total if total else 0
    logging.info(
        f"Processed {filepath}: {total} rows, {errors} errors ({err_rate:.2%})"
    )

    if err_rate < NORMAL_ERR_RATE:
        os.rename(
            filepath,
            os.path.join(os.path.dirname(filepath), f".{os.path.basename(filepath)}"),
        )
        logging.info(f"Renamed {filepath} -> .{os.path.basename(filepath)}")
    else:
        logging.warning(f"Too many errors in {filepath}: not renamed")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pattern", required=True)
    parser.add_argument("--dry", action="store_true")
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--log")
    for dt in devtype_to_memc:
        parser.add_argument(f"--{dt}", default="127.0.0.1:33013")
    args = parser.parse_args()

    # Уровень логирования зависит от dry-режима
    log_level = logging.DEBUG if args.dry else logging.INFO
    logging.basicConfig(
        filename=args.log,
        level=log_level,
        format="[%(asctime)s] %(levelname).1s %(message)s",
    )

    logging.info(f"Memc loader started with options: {vars(args)}")

    # Инициализация клиентов memcached
    for devtype in devtype_to_memc:
        addr = getattr(args, devtype)
        devtype_to_memc[devtype] = memcache.Client([addr])

    # Поиск и обработка файлов
    files = sorted(glob.glob(args.pattern))
    for filepath in files:
        process_file(filepath, dry=args.dry, workers=args.workers)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
