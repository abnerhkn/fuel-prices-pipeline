import sys
import os

sys.path.append(os.path.dirname(__file__))

from etl.pipeline import ETLPipeline


def main():

    pipeline = ETLPipeline(year=2025)
    pipeline.run()


if __name__ == "__main__":
    main()
