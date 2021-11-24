import sys

from pylint import lint

THRESHOLD = 9
# pylint: disable=too-many-arguments

run = lint.Run(["report_generator.py"], do_exit=False)

score = run.linter.stats["global_note"]

if score < THRESHOLD:

    print("Linter failed: Score < threshold value")

    sys.exit(1)


sys.exit(0)
