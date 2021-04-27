import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse
import re

def main():
    parser = argparse.ArgumentParser(description='A beam pipeline!')
    parser.add_argument('--input', '-i', help='Input text location')
    parser.add_argument('--output', '-o', help='Output text location')
    parser.add_argument('--nwords', '-n', type=int, help='Number of words in output', default=50)
    our_args, dataflow_args = parser.parse_known_args()
    run_pipeline(our_args, dataflow_args)


def format_output(sorted_words):
    out = ''
    for i, (w, n) in enumerate(sorted_words):
        out += f'{i + 1}: {w:<20}count of {n:,}\n'
    return out
    # return '\n'.join([f"{w},{n}" for w, n in sorted_words])

def sanitise_word(w):
    return ''.join([l for l in w if l.isalnum()]).lower()

def run_pipeline(custom_args, runner_args):
    opts = PipelineOptions(runner_args)

    with beam.Pipeline(options=opts) as p:
        # (p.apply(some_func, 'some_label')) == (p | 'some_label' >> some_func)
        lines = p | 'Read input text' >> beam.io.ReadFromText(custom_args.input)
        # lines is a PCollection object. It's immutable and unordered
        words = lines | 'Split' >> beam.FlatMap(lambda x: x.split())
        # FlatMap is like a tuple with cardinality > 1 (unlike a list which has cardinality 1 and elements > 1)
        sanitised = words | 'Sanitise words' >> beam.Map(sanitise_word)
        counts = sanitised | 'Count words' >> beam.combiners.Count.PerElement()
        # Combiners are more scalable than 'group by' keys, but harder to write
        top_words = counts | 'Find top 50' >> beam.combiners.Top.Of(custom_args.nwords, key=lambda x:x[1])
        top_words | beam.Map(format_output) | beam.Map(print)
        # top_words | 'Save output' >> beam.io.WriteToText(custom_args.output)

if __name__ == '__main__':
    main()
