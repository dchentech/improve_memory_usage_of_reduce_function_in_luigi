# -*- coding: utf-8 -*-

from __future__ import print_function
import json
from collections import defaultdict

from itertools import groupby
import sys
import unittest


"""
### luigi executing flow.

    1. `luigid` package all your python code and library into a package tar.
    2. `luigid` submit this job to YARN when it's this job's turn.
    3. `YARN` distribute this job with parameters and files to each nodes.
    4. Each `YARN` node unpack the pacakge tar, and begin to run
       `luigi/mrrunner.py`. If current compute type is `reduce`, then the code
       `mrrunner.py` will run `self.job.run_reducer(stdin, stdout)`.
    5. `run_reducer` will call `self.reducer` function, that's all.


### luigi `reduce` limit?

    `reduce` use three generator attempt to reduce the memory cost lazily, but
    it maybe failed.

    1. Reader(lines come from stdin) generator.
       `self.internal_reader((line[:-1] for line in stdin)`
    2. Groupby(by the key from mapper) generator.
       `for key, values in groupby(inputs, key=lambda x: repr(x[0]))`
    3. Reducer(the actually reducer function that we defined in the task)
       generator.
       `for output in reducer(eval(key), (v[1] for v in values))`

    Because `Groupby` generator means to read all of the data, and that's why
    `reducer` start to run after `mapper` is 100% finished ...


    JobTask and BaseHadoopJobTask are luigi.task.

### related code in luigi.

1. luigi/mrrunner.py
```python
class Runner(object):
    def run(self, kind, stdin=sys.stdin, stdout=sys.stdout):
        if kind == "map":
            self.job.run_mapper(stdin, stdout)
        elif kind == "combiner":
            self.job.run_combiner(stdin, stdout)
        elif kind == "reduce":
            self.job.run_reducer(stdin, stdout)
        else:
            raise Exception('weird command: %s' % kind)
```

2. luigi/hadoop.py
```python
class BaseHadoopJobTask(luigi.Task):
    pass

class JobTask(BaseHadoopJobTask):
    def run_reducer(self, stdin=sys.stdin, stdout=sys.stdout):
        self.init_hadoop()
        self.init_reducer()
        outputs = self._reduce_input( \
                        self.internal_reader((line[:-1] for line in stdin)), \
                        self.reducer, self.final_reducer)
        self.writer(outputs, stdout)

    def _reduce_input(self, inputs, reducer, final=NotImplemented):
        for key, values in groupby(inputs, key=lambda x: repr(x[0])):
            for output in reducer(eval(key), (v[1] for v in values)):
                yield output
        if final != NotImplemented:
            for output in final():
                yield output
        self._flush_batch_incr_counter()

    def internal_reader(self, input_stream):
        for input_line in input_stream:
            yield list(map(eval, input_line.split("\t")))

    def writer(self, outputs, stdout, stderr=sys.stderr):
        for output in outputs:
            try:
                print("\t".join(map(str, flatten(output))), file=stdout)
            except:
                print(output, file=stderr)
                raise
```
"""


def improve_memory_usage_in_reduce(task_cls):

    # luigi use `eval`, but luiti use the subset json.
    deserialize_function = lambda self, v1: json.loads(v1)
    deserialize_function = lambda self, v1: eval(v1)
    default_line_separator = "\t"                         # cause luigi use it.

    def _reduce_input(self, inputs, reducer, final=NotImplemented):
        """
        Iterate over input, collect values with the same key, and call the
        reducer for each unique key.
        """
        # load all data in one reducer process directly.
        key_to_vals_dict = defaultdict(list)
        for k1, v1 in inputs:
            key_to_vals_dict[k1].append(v1)
        # list use less memory than dict
        # TODO use c library to reduce more memory.
        key_to_vals_lines = key_to_vals_dict.items()
        del key_to_vals_dict

        # process lines by `reducer` function
        for key_to_vals_1 in key_to_vals_lines:
            k2 = self.deserialize_function(key_to_vals_1[0])
            # load vals by key lazily, because convert str to complex is need
            # more memory.
            vs2 = map(self.deserialize_function, key_to_vals_1[1])
            for output3 in self.reducer(k2, vs2):
                yield output3

        # the original code from luigi.
        if final != NotImplemented:
            for output in final():
                yield output
        self._flush_batch_incr_counter()

    def internal_reader(self, input_stream):
        for input_line in input_stream:
            yield list(input_line.split(self.default_line_separator, 1))

    for func in ["deserialize_function",
                 "default_line_separator",
                 "_reduce_input",
                 "internal_reader", ]:
        setattr(task_cls, func, locals()[func])

    return task_cls

# improve_memory_usage_in_reduce(JobTask)


class MockLuigiLazyGenerator(object):

    sorted_inputs = iter("""
"a"\t1
"a"\t2
"b"\t1
"c"\t1
"d"\t1
"e"\t1
"e"\t2
"f"\t1
"a"\t1
""".strip().split("\n"))

    process_logs = list()

    def _reduce_input(self, inputs, reducer):
        for key, values in groupby(inputs, key=lambda x: repr(x[0])):
            self.process_logs.append({"key": key, "type": "groupby"})
            for output in reducer(eval(key), (v[1] for v in values)):
                self.process_logs.append({"key": key, "type": "reducer"})
                yield output

    def internal_reader(self, input_stream):
        for input_line in input_stream:
            yield list(map(eval, input_line.split("\t")))

    def reducer(self, k, vs):
        yield k, sum(vs)

    def run_reducer(self, stdin=sys.stdin, stdout=sys.stdout):
        outputs = self._reduce_input(
            self.internal_reader((line for line in stdin)),
            self.reducer
            )
        return list(outputs)


class TestMain(unittest.TestCase):

    def test_main(self):
        mock = MockLuigiLazyGenerator()
        result = mock.run_reducer(mock.sorted_inputs)
        self.assertEqual(
            result,
            {"a": 4, "b": 1, "c": 1, "d": 1, "e": 3, "f": 1},)
        print("[mock.process_logs]", mock.process_logs)

if __name__ == '__main__':
    unittest.main()

"""
wired result!!!

result is
[('a', 3), ('b', 1), ('c', 1), ('d', 1), ('e', 3), ('f', 1), ('a', 1)]

(Pdb) aa = list(groupby(inputs, key=lambda x: repr(x[0])))
(Pdb) aa
[("'a'", <itertools._grouper object at 0x106099090>),
 ("'b'", <itertools._grouper object at 0x106099150>),
 ("'c'", <itertools._grouper object at 0x106099190>),
 ("'d'", <itertools._grouper object at 0x1060991d0>),
 ("'e'", <itertools._grouper object at 0x106099210>),
 ("'f'", <itertools._grouper object at 0x106099250>),
 ("'a'", <itertools._grouper object at 0x106099290>)]
"""
