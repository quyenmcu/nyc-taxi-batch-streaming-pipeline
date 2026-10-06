"""Paste into a Mage custom block after making pipeline.py importable."""
import os
from pipeline import batch
if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom

@custom
def run_batch(*args, **kwargs):
    return batch(os.environ.get('TAXI_CSV', 'data/sample.csv'),
                 os.environ.get('TAXI_DB', 'output/taxi.db'))
