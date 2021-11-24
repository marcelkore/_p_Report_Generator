import os
import sys

from pylint import lint

BUCKET_NAME = os.environ.get("bucket_name")

THRESHOLD = 9
# pylint: disable=too-many-arguments

run = lint.Run(["report_generator.py"], do_exit=False)

score = run.linter.stats["global_note"]

print(f"Bucket Name {BUCKET_NAME}")

if score < THRESHOLD:

    print("Linter failed: Score < threshold value")

    sys.exit(1)


sys.exit(0)
