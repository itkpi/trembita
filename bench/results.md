# Results: word count
```
=== AVG time (scala 2.11.8)
Benchmark                                         Mode  Cnt       Score       Error  Units
KernelWordsCountBench.vectorLargeFile             avgt   30  303173,748 ±  8159,947  us/op
KernelWordsCountBench.pipelineNaiveLargeFile      avgt   30  331177,860 ±  9973,847  us/op
KernelWordsCountBench.pipelinesAdvancedLargeFile  avgt   30  555365,298 ± 12958,766  us/op
KernelWordsCountBench.imperativeLargeFile         avgt   30  603611,411 ± 52721,372  us/op

KernelWordsCountBench.vectorMiddleFile            avgt   30     230,569 ±     1,550  us/op
KernelWordsCountBench.pipelineNaiveMiddleFile     avgt   30     233,724 ±     1,866  us/op
KernelWordsCountBench.imperativeMiddleFile        avgt   30     245,146 ±     9,431  us/op
KernelWordsCountBench.pipelineAdvancedMiddleFile  avgt   30    1409,638 ±    35,590  us/op

KernelWordsCountBench.imperativeExtraLargeFile        avgt   30  638,066 ± 15,587  ms/op
KernelWordsCountBench.pipelineNaiveExtraLargeFile     avgt   30  638,308 ± 11,759  ms/op
KernelWordsCountBench.vectorExtraLargeFile            avgt   30  645,671 ±  5,594  ms/op
KernelWordsCountBench.pipelinesAdvanceExtraLargeFile  avgt   30  646,305 ±  11,635  ms/op

KernelWordsCountBench.vectorOhMyGoodness              avgt   30  5004,778 ± 126,594  ms/op
KernelWordsCountBench.pipelineNaiveOhMyGoodness       avgt   30  5077,192 ±  90,466  ms/op
KernelWordsCountBench.imperativeOhMyGoodnessFile      avgt   30  5310,029 ±  86,176  ms/op
KernelWordsCountBench.pipelinesAdvanceOhMyGoodness    avgt   30  5419,266 ±  96,229  ms/op
```
