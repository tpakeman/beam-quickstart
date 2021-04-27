import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse

def main():
    parser = argparse.ArgumentParser(description='A beam pipeline!')
    parser.add_argument('--input', '-i', help='input textl location')
    parser.add_argument('--output', '-o', help='input textl location')
    our_args, dataflow_args = parser.parse_known_args()
    run_pipeline(our_args, dataflow_args)


def format_output(sorted_words):
    return ' '.join([f"{w},{n}" for w, n in sorted_words])

def run_pipeline(custom_args, runner_args):
    opts = PipelineOptions(runner_args)
    infile = custom_args.input
    outfile = custom_args.output

    with beam.Pipeline(options=opts) as p:
        # (p.apply(some_func, 'some_label')) == (p | 'some_label' >> some_func)
        lines = p | 'Read input text' >> beam.io.ReadFromText(infile)
        # lines is a PCollection object. It's immutable and unordered
        words = lines | 'Split' >> beam.FlatMap(lambda x: x.split())
        # FlatMap is like a tuple with cardinality > 1 (unlike a list which has cardinality 1 and elements > 1)
        counts = words | 'Count words' >> beam.combiners.Count.PerElement()
        # Combiners are more scalable than 'group by' keys, but harder to write
        top_words = counts | 'Find top 50' >> beam.combiners.Top.Of(50, key=lambda x:x[1])
        top_words | beam.Map(format_output) | beam.Map(print)
        # top_words | 'Save output' >> beam.io.WriteToText(outfile)

if __name__ == '__main__':
    main()
