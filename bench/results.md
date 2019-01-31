# Results: word count
```
=== AVG time (scala 2.11.8) Vectors
[info] Benchmark                                         Mode  Cnt       Score       Error  Units
[info] KernelWordsCountBench.vectorMiddleFile            avgt  200     242,126 ±     4,480  us/op
[info] KernelWordsCountBench.imperativeMiddleFile        avgt  200     265,141 ±    16,806  us/op
[info] KernelWordsCountBench.pipelineNaiveMiddleFile     avgt  200     279,495 ±    13,810  us/op
[info] KernelWordsCountBench.pipelineAdvancedMiddleFile  avgt  200    1274,492 ±    71,240  us/op

[info] KernelWordsCountBench.pipelinesAdvancedLargeFile  avgt  200  320722,195 ±  7002,904  us/op
[info] KernelWordsCountBench.vectorLargeFile             avgt  200  354740,033 ± 11436,666  us/op
[info] KernelWordsCountBench.pipelineNaiveLargeFile      avgt  200  436694,191 ± 14781,783  us/op
[info] KernelWordsCountBench.imperativeLargeFile         avgt  200  691346,871 ±  3800,458  us/op

[info] KernelWordsCountBench.pipelineNaiveExtraLargeFile     avgt   30     628,141 ±   25,369  ms/op
[info] KernelWordsCountBench.imperativeExtraLargeFile        avgt   30     641,539 ±   34,984  ms/op
[info] KernelWordsCountBench.vectorExtraLargeFile            avgt   30     708,016 ±   82,627  ms/op
[info] KernelWordsCountBench.pipelinesAdvanceExtraLargeFile  avgt   30     726,254 ±   53,225  ms/op

[info] KernelWordsCountBench.imperativeOhMyGoodnessFile      avgt   30  121644,406 ± 5927,123  ms/op

```
